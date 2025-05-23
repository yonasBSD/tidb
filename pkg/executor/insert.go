// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"encoding/hex"
	"fmt"
	"runtime/trace"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

// InsertExec represents an insert executor.
type InsertExec struct {
	*InsertValues
	OnDuplicate    []*expression.Assignment
	evalBuffer4Dup chunk.MutRow
	curInsertVals  chunk.MutRow
	row4Update     []types.Datum

	Priority mysql.PriorityEnum
}

func (e *InsertExec) exec(ctx context.Context, rows [][]types.Datum) error {
	defer trace.StartRegion(ctx, "InsertExec").End()
	logutil.Eventf(ctx, "insert %d rows into table `%s`", len(rows), stringutil.StringerFunc(func() string {
		var tblName string
		if meta := e.Table.Meta(); meta != nil {
			tblName = meta.Name.L
		}
		return tblName
	}))
	// If tidb_batch_insert is ON and not in a transaction, we could use BatchInsert mode.
	sessVars := e.Ctx().GetSessionVars()
	defer sessVars.CleanBuffers()

	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return err
	}
	setOptionForTopSQL(sessVars.StmtCtx, txn)
	if e.collectRuntimeStatsEnabled() {
		if snapshot := txn.GetSnapshot(); snapshot != nil {
			snapshot.SetOption(kv.CollectRuntimeStats, e.stats.SnapshotRuntimeStats)
			defer snapshot.SetOption(kv.CollectRuntimeStats, nil)
		}
	}
	sessVars.StmtCtx.AddRecordRows(uint64(len(rows)))
	// If you use the IGNORE keyword, duplicate-key error that occurs while executing the INSERT statement are ignored.
	// For example, without IGNORE, a row that duplicates an existing UNIQUE index or PRIMARY KEY value in
	// the table causes a duplicate-key error and the statement is aborted. With IGNORE, the row is discarded and no error occurs.
	// However, if the `on duplicate update` is also specified, the duplicated row will be updated.
	// Using BatchGet in insert ignore to mark rows as duplicated before we add records to the table.
	// If `ON DUPLICATE KEY UPDATE` is specified, and no `IGNORE` keyword,
	// the to-be-insert rows will be check on duplicate keys and update to the new rows.
	if len(e.OnDuplicate) > 0 {
		err := e.batchUpdateDupRows(ctx, rows)
		if err != nil {
			return err
		}
	} else if e.ignoreErr {
		err := e.batchCheckAndInsert(ctx, rows, e.addRecord, false)
		if err != nil {
			return err
		}
	} else {
		start := time.Now()
		dupKeyCheck := optimizeDupKeyCheckForNormalInsert(sessVars, txn)
		for i, row := range rows {
			var err error
			sizeHintStep := int(sessVars.ShardAllocateStep)
			if i%sizeHintStep == 0 {
				sizeHint := sizeHintStep
				remain := len(rows) - i
				if sizeHint > remain {
					sizeHint = remain
				}
				err = e.addRecordWithAutoIDHint(ctx, row, sizeHint, dupKeyCheck)
			} else {
				err = e.addRecord(ctx, row, dupKeyCheck)
			}
			if err != nil {
				return err
			}
		}
		if e.stats != nil {
			e.stats.CheckInsertTime += time.Since(start)
		}
	}
	return txn.MayFlush()
}

func prefetchUniqueIndices(ctx context.Context, txn kv.Transaction, rows []toBeCheckedRow) (map[string][]byte, error) {
	r, ctx := tracing.StartRegionEx(ctx, "prefetchUniqueIndices")
	defer r.End()

	nKeys := 0
	for _, r := range rows {
		if r.ignored {
			continue
		}
		if r.handleKey != nil {
			nKeys++
		}
		nKeys += len(r.uniqueKeys)
	}
	batchKeys := make([]kv.Key, 0, nKeys)
	for _, r := range rows {
		if r.ignored {
			continue
		}
		if r.handleKey != nil {
			batchKeys = append(batchKeys, r.handleKey.newKey)
		}
		for _, k := range r.uniqueKeys {
			batchKeys = append(batchKeys, k.newKey)
		}
	}
	return txn.BatchGet(ctx, batchKeys)
}

func prefetchConflictedOldRows(ctx context.Context, txn kv.Transaction, rows []toBeCheckedRow, values map[string][]byte) error {
	r, ctx := tracing.StartRegionEx(ctx, "prefetchConflictedOldRows")
	defer r.End()

	batchKeys := make([]kv.Key, 0, len(rows))
	for _, r := range rows {
		for _, uk := range r.uniqueKeys {
			if val, found := values[string(uk.newKey)]; found {
				if tablecodec.IsTempIndexKey(uk.newKey) {
					// If it is a temp index, the value cannot be decoded by DecodeHandleInIndexValue.
					// Since this function is an optimization, we can skip prefetching the rows referenced by
					// temp indexes.
					continue
				}
				handle, err := tablecodec.DecodeHandleInIndexValue(val)
				if err != nil {
					return err
				}
				batchKeys = append(batchKeys, tablecodec.EncodeRecordKey(r.t.RecordPrefix(), handle))
			}
		}
	}
	_, err := txn.BatchGet(ctx, batchKeys)
	return err
}

func (e *InsertValues) prefetchDataCache(ctx context.Context, txn kv.Transaction, rows []toBeCheckedRow) error {
	// Temporary table need not to do prefetch because its all data are stored in the memory.
	if e.Table.Meta().TempTableType != model.TempTableNone {
		return nil
	}

	defer tracing.StartRegion(ctx, "prefetchDataCache").End()
	values, err := prefetchUniqueIndices(ctx, txn, rows)
	if err != nil {
		return err
	}
	return prefetchConflictedOldRows(ctx, txn, rows, values)
}

// updateDupRow updates a duplicate row to a new row.
func (e *InsertExec) updateDupRow(
	ctx context.Context,
	idxInBatch int,
	txn kv.Transaction,
	row toBeCheckedRow,
	handle kv.Handle,
	_ []*expression.Assignment,
	dupKeyCheck table.DupKeyCheckMode,
	autoColIdx int,
) error {
	oldRow, err := getOldRow(ctx, e.Ctx(), txn, row.t, handle, e.GenExprs)
	if err != nil {
		return err
	}
	// get the extra columns from the SELECT clause.
	var extraCols []types.Datum
	if len(e.Ctx().GetSessionVars().CurrInsertBatchExtraCols) > 0 {
		extraCols = e.Ctx().GetSessionVars().CurrInsertBatchExtraCols[idxInBatch]
	}

	err = e.doDupRowUpdate(ctx, handle, oldRow, row.row, extraCols, e.OnDuplicate, idxInBatch, dupKeyCheck, autoColIdx)
	if kv.ErrKeyExists.Equal(err) || table.ErrCheckConstraintViolated.Equal(err) {
		ec := e.Ctx().GetSessionVars().StmtCtx.ErrCtx()
		return ec.HandleErrorWithAlias(kv.ErrKeyExists, err, err)
	}
	return err
}

// batchUpdateDupRows updates multi-rows in batch if they are duplicate with rows in table.
func (e *InsertExec) batchUpdateDupRows(ctx context.Context, newRows [][]types.Datum) error {
	// Get keys need to be checked.
	start := time.Now()
	toBeCheckedRows, err := getKeysNeedCheck(e.Ctx(), e.Table, newRows)
	if err != nil {
		return err
	}

	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return err
	}

	prefetchStart := time.Now()
	// Use BatchGet to fill cache.
	// It's an optimization and could be removed without affecting correctness.
	if err = e.prefetchDataCache(ctx, txn, toBeCheckedRows); err != nil {
		return err
	}
	if e.stats != nil {
		e.stats.Prefetch += time.Since(prefetchStart)
	}

	// Use `optimizeDupKeyCheckForUpdate` to determine the update operation when the row meets the conflict in
	// `INSERT ... ON DUPLICATE KEY UPDATE` statement.
	// Though it is in an insert statement, `ON DUP KEY UPDATE` follows the dup-key check behavior of update.
	// For example, it will ignore variable `tidb_constraint_check_in_place`, see the test case:
	// https://github.com/pingcap/tidb/blob/3117d3fae50bbb5dabcde7b9589f92bfbbda5dc6/pkg/executor/test/writetest/write_test.go#L419-L426
	updateDupKeyCheck := optimizeDupKeyCheckForUpdate(txn, e.ignoreErr)
	// Do not use `updateDupKeyCheck` for `AddRecord` because it is not optimized for insert.
	// It seems that we can just use `DupKeyCheckSkip` here because all constraints are checked.
	// But we still use `optimizeDupKeyCheckForNormalInsert` to make the refactor same behavior with the original code.
	// TODO: just use `DupKeyCheckSkip` here.
	addRecordDupKeyCheck := optimizeDupKeyCheckForNormalInsert(e.Ctx().GetSessionVars(), txn)

	_, autoColIdx, found := findAutoIncrementColumn(e.Table)
	if !found {
		autoColIdx = -1
	}

	for i, r := range toBeCheckedRows {
		if r.handleKey != nil {
			handle, err := tablecodec.DecodeRowKey(r.handleKey.newKey)
			if err != nil {
				return err
			}

			err = e.updateDupRow(ctx, i, txn, r, handle, e.OnDuplicate, updateDupKeyCheck, autoColIdx)
			if err == nil {
				continue
			}
			if !kv.IsErrNotFound(err) {
				return err
			}
		}

		for _, uk := range r.uniqueKeys {
			handle, err := tables.FetchDuplicatedHandle(ctx, uk.newKey, txn)
			if err != nil {
				return err
			}
			if handle == nil {
				continue
			}
			err = e.updateDupRow(ctx, i, txn, r, handle, e.OnDuplicate, updateDupKeyCheck, autoColIdx)
			if err != nil {
				if kv.IsErrNotFound(err) {
					// Data index inconsistent? A unique key provide the handle information, but the
					// handle points to nothing.
					logutil.BgLogger().Error("get old row failed when insert on dup",
						zap.String("uniqueKey", hex.EncodeToString(uk.newKey)),
						zap.Stringer("handle", handle),
						zap.String("toBeInsertedRow", types.DatumsToStrNoErr(r.row)))
				}
				return err
			}

			newRows[i] = nil
			break
		}

		// If row was checked with no duplicate keys,
		// we should do insert the row,
		// and key-values should be filled back to dupOldRowValues for the further row check,
		// due to there may be duplicate keys inside the insert statement.
		if newRows[i] != nil {
			err := e.addRecord(ctx, newRows[i], addRecordDupKeyCheck)
			if err != nil {
				return err
			}
		}
	}
	if e.stats != nil {
		e.stats.CheckInsertTime += time.Since(start)
	}
	return nil
}

// optimizeDupKeyCheckForNormalInsert trys to optimize the DupKeyCheckMode for an insert statement according to the
// transaction and system variables.
// If the DupKeyCheckMode of the current statement can be optimized, it will return `DupKeyCheckLazy` to avoid the
// redundant requests to TiKV, otherwise, `DupKeyCheckInPlace` will be returned.
// This method only works for "normal" insert statements, that means the options like "IGNORE" and "ON DUPLICATE KEY"
// in a statement are not considerate, and callers should handle the above cases by themselves.
func optimizeDupKeyCheckForNormalInsert(vars *variable.SessionVars, txn kv.Transaction) table.DupKeyCheckMode {
	if !vars.ConstraintCheckInPlace || txn.IsPessimistic() || txn.IsPipelined() {
		// We can just check duplicated key lazily without keys in storage for the below cases:
		// - `txn.Pipelined()` is true.
		//    It means the user is using `@@tidb_dml_type="bulk"` to insert rows in bulk mode.
		//    DupKeyCheckLazy should be used to improve the performance.
		// - The current transaction is pessimistic. The duplicate key check can be postponed to the lock stage.
		// - The current transaction is optimistic but `tidb_constraint_check_in_place` is set to false.
		return table.DupKeyCheckLazy
	}
	return table.DupKeyCheckInPlace
}

// getPessimisticLazyCheckMode returns the lazy check mode for pessimistic txn.
// The returned `PessimisticLazyDupKeyCheckMode` only takes effect for pessimistic txn with `DupKeyCheckLazy`;
// otherwise, this option will be ignored.
func getPessimisticLazyCheckMode(vars *variable.SessionVars) table.PessimisticLazyDupKeyCheckMode {
	if !vars.ConstraintCheckInPlacePessimistic && vars.InTxn() && !vars.InRestrictedSQL && vars.ConnectionID > 0 {
		// We can postpone the duplicated key check to the prewrite stage when both of the following conditions are met:
		// - `tidb_constraint_check_in_place_pessimistic='OFF'`.
		// - The current transaction should be an explicit transaction because an autocommit txn cannot get
		//   any benefits from checking the duplicated key in the prewrite stage.
		// - The current connection is a user connection, and we always check duplicated key in place for
		//   internal connections.
		return table.DupKeyCheckInPrewrite
	}
	return table.DupKeyCheckInAcquireLock
}

// Next implements the Executor Next interface.
func (e *InsertExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.collectRuntimeStatsEnabled() {
		ctx = context.WithValue(ctx, autoid.AllocatorRuntimeStatsCtxKey, e.stats.AllocatorRuntimeStats)
	}

	if !e.EmptyChildren() && e.Children(0) != nil {
		return insertRowsFromSelect(ctx, e)
	}
	err := insertRows(ctx, e)
	if err != nil {
		terr, ok := errors.Cause(err).(*terror.Error)
		if ok && len(e.OnDuplicate) == 0 && terr.Code() == errno.ErrAutoincReadFailed {
			ec := e.Ctx().GetSessionVars().StmtCtx.ErrCtx()
			return ec.HandleError(err)
		}
		return err
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *InsertExec) Close() error {
	if e.RuntimeStats() != nil && e.stats != nil {
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.stats)
	}
	defer e.memTracker.ReplaceBytesUsed(0)
	e.setMessage()
	if e.SelectExec != nil {
		return exec.Close(e.SelectExec)
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *InsertExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	if e.OnDuplicate != nil {
		e.initEvalBuffer4Dup()
	}
	if e.SelectExec != nil {
		return exec.Open(ctx, e.SelectExec)
	}
	if !e.allAssignmentsAreConstant {
		e.initEvalBuffer()
	}
	return nil
}

func (e *InsertExec) initEvalBuffer4Dup() {
	// Use public columns for new row.
	numCols := len(e.Table.Cols())
	// Use writable columns for old row for update.
	numWritableCols := len(e.Table.WritableCols())

	extraLen := 0
	if e.SelectExec != nil {
		extraLen = e.SelectExec.Schema().Len() - e.rowLen
	}

	evalBufferTypes := make([]*types.FieldType, 0, numCols+numWritableCols+extraLen)

	// Append the old row before the new row, to be consistent with "Schema4OnDuplicate" in the "Insert" PhysicalPlan.
	for _, col := range e.Table.WritableCols() {
		evalBufferTypes = append(evalBufferTypes, &(col.FieldType))
	}
	if extraLen > 0 {
		evalBufferTypes = append(evalBufferTypes, e.SelectExec.RetFieldTypes()[e.rowLen:]...)
	}
	for _, col := range e.Table.Cols() {
		evalBufferTypes = append(evalBufferTypes, &(col.FieldType))
	}
	if e.hasExtraHandle {
		evalBufferTypes = append(evalBufferTypes, types.NewFieldType(mysql.TypeLonglong))
	}
	e.evalBuffer4Dup = chunk.MutRowFromTypes(evalBufferTypes)
	e.curInsertVals = chunk.MutRowFromTypes(evalBufferTypes[numWritableCols+extraLen:])
	e.row4Update = make([]types.Datum, 0, len(evalBufferTypes))
}

// doDupRowUpdate updates the duplicate row.
func (e *InsertExec) doDupRowUpdate(
	ctx context.Context,
	handle kv.Handle,
	oldRow, newRow, extraCols []types.Datum,
	assigns []*expression.Assignment,
	idxInBatch int,
	dupKeyMode table.DupKeyCheckMode,
	autoColIdx int,
) error {
	assignFlag := make([]bool, len(e.Table.WritableCols()))
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	e.curInsertVals.SetDatums(newRow...)
	e.Ctx().GetSessionVars().CurrInsertValues = e.curInsertVals.ToRow()
	// NOTE: In order to execute the expression inside the column assignment,
	// we have to put the value of "oldRow" and "extraCols" before "newRow" in
	// "row4Update" to be consistent with "Schema4OnDuplicate" in the "Insert"
	// PhysicalPlan.
	e.row4Update = e.row4Update[:0]
	e.row4Update = append(e.row4Update, oldRow...)
	e.row4Update = append(e.row4Update, extraCols...)
	e.row4Update = append(e.row4Update, newRow...)

	// Only evaluate non-generated columns here,
	// other fields will be evaluated in updateRecord.
	var generated, nonGenerated []*expression.Assignment
	cols := e.Table.Cols()
	for _, assign := range assigns {
		if cols[assign.Col.Index].IsGenerated() {
			generated = append(generated, assign)
		} else {
			nonGenerated = append(nonGenerated, assign)
		}
	}

	warnCnt := int(e.Ctx().GetSessionVars().StmtCtx.WarningCount())
	errorHandler := func(sctx sessionctx.Context, assign *expression.Assignment, val *types.Datum, err error) error {
		c := assign.Col.ToInfo()
		c.Name = assign.ColName
		sc := sctx.GetSessionVars().StmtCtx

		if newWarnings := sc.TruncateWarnings(warnCnt); len(newWarnings) > 0 {
			for k := range newWarnings {
				// Use `idxInBatch` here for simplicity, since the offset of the batch is unknown under the current context.
				newWarnings[k].Err = completeInsertErr(c, val, idxInBatch, newWarnings[k].Err)
			}
			sc.AppendWarnings(newWarnings)
			warnCnt += len(newWarnings)
		}
		return err
	}

	// Update old row when the key is duplicated.
	e.evalBuffer4Dup.SetDatums(e.row4Update...)
	sctx := e.Ctx()
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	for _, assign := range nonGenerated {
		var val types.Datum
		if assign.LazyErr != nil {
			return assign.LazyErr
		}
		val, err := assign.Expr.Eval(evalCtx, e.evalBuffer4Dup.ToRow())
		if err != nil {
			return err
		}

		c := assign.Col.ToInfo()
		idx := assign.Col.Index
		c.Name = assign.ColName
		val, err = table.CastValue(sctx, val, c, false, false)
		if err != nil {
			return err
		}

		_ = errorHandler(sctx, assign, &val, nil)
		e.evalBuffer4Dup.SetDatum(idx, val)
		e.row4Update[assign.Col.Index] = val
		assignFlag[assign.Col.Index] = true
	}

	newData := e.row4Update[:len(oldRow)]
	_, ignored, err := updateRecord(
		ctx, e.Ctx(),
		handle, oldRow, newData,
		0, generated, e.evalBuffer4Dup, errorHandler,
		assignFlag, e.Table,
		true, e.memTracker, e.fkChecks, e.fkCascades, dupKeyMode, e.ignoreErr)

	if ignored {
		return nil
	}

	if err != nil {
		return errors.Trace(err)
	}

	if autoColIdx >= 0 {
		if e.Ctx().GetSessionVars().StmtCtx.AffectedRows() > 0 {
			// If "INSERT ... ON DUPLICATE KEY UPDATE" duplicate and update a row,
			// auto increment value should be set correctly for mysql_insert_id()
			// See https://github.com/pingcap/tidb/issues/55965
			e.Ctx().GetSessionVars().StmtCtx.InsertID = newData[autoColIdx].GetUint64()
		} else {
			e.Ctx().GetSessionVars().StmtCtx.InsertID = 0
		}
	}
	return nil
}

// setMessage sets info message(ERR_INSERT_INFO) generated by INSERT statement
func (e *InsertExec) setMessage() {
	stmtCtx := e.Ctx().GetSessionVars().StmtCtx
	numRecords := stmtCtx.RecordRows()
	if e.SelectExec != nil || numRecords > 1 {
		numWarnings := stmtCtx.WarningCount()
		var numDuplicates uint64
		if e.ignoreErr {
			// if ignoreErr
			numDuplicates = numRecords - stmtCtx.CopiedRows()
		} else {
			if e.Ctx().GetSessionVars().ClientCapability&mysql.ClientFoundRows > 0 {
				numDuplicates = stmtCtx.TouchedRows()
			} else {
				numDuplicates = stmtCtx.UpdatedRows()
			}
		}
		msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrInsertInfo].Raw, numRecords, numDuplicates, numWarnings)
		stmtCtx.SetMessage(msg)
	}
}

// GetFKChecks implements WithForeignKeyTrigger interface.
func (e *InsertExec) GetFKChecks() []*FKCheckExec {
	return e.fkChecks
}

// GetFKCascades implements WithForeignKeyTrigger interface.
func (e *InsertExec) GetFKCascades() []*FKCascadeExec {
	return e.fkCascades
}

// HasFKCascades implements WithForeignKeyTrigger interface.
func (e *InsertExec) HasFKCascades() bool {
	return len(e.fkCascades) > 0
}

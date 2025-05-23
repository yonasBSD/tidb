// Copyright 2016 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/tikv/client-go/v2/oracle/oracles"
	"go.uber.org/zap"
)

// SetExecutor executes set statement.
type SetExecutor struct {
	exec.BaseExecutor

	vars []*expression.VarAssignment
	done bool
}

// Next implements the Executor Next interface.
func (e *SetExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	sctx := e.Ctx()
	sessionVars := sctx.GetSessionVars()
	for _, v := range e.vars {
		// Variable is case insensitive, we use lower case.
		if v.Name == ast.SetNames || v.Name == ast.SetCharset {
			// This is set charset stmt.
			if v.IsDefault {
				err := e.setCharset(mysql.DefaultCharset, "", v.Name == ast.SetNames)
				if err != nil {
					return err
				}
				continue
			}
			dt, err := v.Expr.(*expression.Constant).Eval(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}
			cs := dt.GetString()
			var co string
			if v.ExtendValue != nil {
				co = v.ExtendValue.Value.GetString()
			}
			err = e.setCharset(cs, co, v.Name == ast.SetNames)
			if err != nil {
				return err
			}
			continue
		}
		name := strings.ToLower(v.Name)
		if !v.IsSystem {
			// Set user variable.
			value, err := v.Expr.Eval(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}
			if value.IsNull() {
				sessionVars.UnsetUserVar(name)
			} else {
				sessionVars.SetUserVarVal(name, value)
				sessionVars.SetUserVarType(name, v.Expr.GetType(sctx.GetExprCtx().GetEvalCtx()))
			}
			continue
		}

		if err := e.setSysVariable(ctx, name, v); err != nil {
			return err
		}
	}
	return nil
}

func (e *SetExecutor) setSysVariable(ctx context.Context, name string, v *expression.VarAssignment) error {
	sessionVars := e.Ctx().GetSessionVars()
	sysVar := variable.GetSysVar(name)
	if sysVar == nil {
		if variable.IsRemovedSysVar(name) {
			return nil // removed vars permit parse-but-ignore
		}
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}

	if sysVar.RequireDynamicPrivileges != nil {
		semEnabled := sem.IsEnabled()
		pm := privilege.GetPrivilegeManager(e.Ctx())
		privs := sysVar.RequireDynamicPrivileges(v.IsGlobal, semEnabled)
		for _, priv := range privs {
			if !pm.RequestDynamicVerification(sessionVars.ActiveRoles, priv, false) {
				msg := priv
				if !semEnabled {
					msg = "SUPER or " + msg
				}
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs(msg)
			}
		}
	}

	if sysVar.IsNoop && !vardef.EnableNoopVariables.Load() {
		// The variable is a noop. For compatibility we allow it to still
		// be changed, but we append a warning since users might be expecting
		// something that's not going to happen.
		sessionVars.StmtCtx.AppendWarning(exeerrors.ErrSettingNoopVariable.FastGenByArgs(sysVar.Name))
	}
	if sysVar.HasInstanceScope() && !v.IsGlobal && sessionVars.EnableLegacyInstanceScope {
		// For backward compatibility we will change the v.IsGlobal to true,
		// and append a warning saying this will not be supported in future.
		v.IsGlobal = true
		sessionVars.StmtCtx.AppendWarning(exeerrors.ErrInstanceScope.FastGenByArgs(sysVar.Name))
	}

	if v.IsGlobal {
		valStr, err := e.getVarValue(ctx, v, sysVar)
		if err != nil {
			return err
		}
		err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(ctx, name, valStr)
		if err != nil {
			return err
		}
		err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
			auditPlugin := plugin.DeclareAuditManifest(p.Manifest)
			if auditPlugin.OnGlobalVariableEvent != nil {
				auditPlugin.OnGlobalVariableEvent(context.Background(), e.Ctx().GetSessionVars(), name, valStr)
			}
			return nil
		})
		showValStr := valStr
		if name == vardef.TiDBCloudStorageURI {
			showValStr = ast.RedactURL(showValStr)
		}
		logutil.BgLogger().Info("set global var", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", showValStr))
		if name == vardef.TiDBServiceScope {
			dom := domain.GetDomain(e.Ctx())
			oldConfig := config.GetGlobalConfig()
			if oldConfig.Instance.TiDBServiceScope != valStr {
				newConfig := *oldConfig
				newConfig.Instance.TiDBServiceScope = valStr
				config.StoreGlobalConfig(&newConfig)
			}
			serverID := disttaskutil.GenerateSubtaskExecID(ctx, dom.DDL().GetID())
			taskMgr, err := storage.GetTaskManager()
			if err != nil {
				return err
			}
			return taskMgr.InitMetaSession(ctx, e.Ctx(), serverID, valStr)
		}
		return err
	}
	// Set session variable
	valStr, err := e.getVarValue(ctx, v, nil)
	if err != nil {
		return err
	}
	getSnapshotTSByName := func() uint64 {
		if name == vardef.TiDBSnapshot {
			return sessionVars.SnapshotTS
		} else if name == vardef.TiDBTxnReadTS {
			return sessionVars.TxnReadTS.PeakTxnReadTS()
		}
		return 0
	}
	oldSnapshotTS := getSnapshotTSByName()
	fallbackOldSnapshotTS := func() {
		if name == vardef.TiDBSnapshot {
			sessionVars.SnapshotTS = oldSnapshotTS
		} else if name == vardef.TiDBTxnReadTS {
			sessionVars.TxnReadTS.SetTxnReadTS(oldSnapshotTS)
		}
	}
	if sessionVars.InTxn() {
		if name == vardef.TxnIsolationOneShot ||
			name == vardef.TiDBTxnReadTS {
			return errors.Trace(exeerrors.ErrCantChangeTxCharacteristics)
		}
		if name == vardef.TiDBSnapshot && sessionVars.TxnCtx.IsStaleness {
			return errors.Trace(exeerrors.ErrCantChangeTxCharacteristics)
		}
	}
	err = sessionVars.SetSystemVar(name, valStr)
	if err != nil {
		return err
	}
	newSnapshotTS := getSnapshotTSByName()
	newSnapshotIsSet := newSnapshotTS > 0 && newSnapshotTS != oldSnapshotTS
	if newSnapshotIsSet {
		isStaleRead := name == vardef.TiDBTxnReadTS
		var ctxForReadTsValidator context.Context
		if !isStaleRead {
			ctxForReadTsValidator = context.WithValue(ctx, oracles.ValidateReadTSForTidbSnapshot{}, struct{}{})
		} else {
			ctxForReadTsValidator = ctx
		}
		err = sessionctx.ValidateSnapshotReadTS(ctxForReadTsValidator, e.Ctx().GetStore(), newSnapshotTS, isStaleRead)
		if name != vardef.TiDBTxnReadTS {
			// Also check gc safe point for snapshot read.
			// We don't check snapshot with gc safe point for read_ts
			// Client-go will automatically check the snapshotTS with gc safe point. It's unnecessary to check gc safe point during set executor.
			if err == nil {
				err = gcutil.ValidateSnapshot(e.Ctx(), newSnapshotTS)
			}
		}
		if err != nil {
			fallbackOldSnapshotTS()
			return err
		}
	}

	err = e.loadSnapshotInfoSchemaIfNeeded(name, newSnapshotTS)
	if err != nil {
		fallbackOldSnapshotTS()
		return err
	}
	// Clients are often noisy in setting session variables such as
	// autocommit, timezone, etc
	logutil.BgLogger().Debug("set session var", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
	return nil
}

func (e *SetExecutor) setCharset(cs, co string, isSetName bool) error {
	var err error
	sessionVars := e.Ctx().GetSessionVars()
	if co == "" {
		if cs == mysql.UTF8MB4Charset {
			co = sessionVars.DefaultCollationForUTF8MB4
		} else if co, err = charset.GetDefaultCollation(cs); err != nil {
			return err
		}
	} else {
		var coll *charset.Collation
		if coll, err = collate.GetCollationByName(co); err != nil {
			return err
		}
		if coll.CharsetName != cs {
			return charset.ErrCollationCharsetMismatch.GenWithStackByArgs(coll.Name, cs)
		}
	}
	if isSetName {
		for _, v := range vardef.SetNamesVariables {
			if err = sessionVars.SetSystemVar(v, cs); err != nil {
				return errors.Trace(err)
			}
		}
		return errors.Trace(sessionVars.SetSystemVar(vardef.CollationConnection, co))
	}
	// Set charset statement, see also https://dev.mysql.com/doc/refman/8.0/en/set-character-set.html.
	for _, v := range vardef.SetCharsetVariables {
		if err = sessionVars.SetSystemVar(v, cs); err != nil {
			return errors.Trace(err)
		}
	}
	csDB, err := sessionVars.GlobalVarsAccessor.GetGlobalSysVar(vardef.CharsetDatabase)
	if err != nil {
		return err
	}
	coDB, err := sessionVars.GlobalVarsAccessor.GetGlobalSysVar(vardef.CollationDatabase)
	if err != nil {
		return err
	}
	err = sessionVars.SetSystemVar(vardef.CharacterSetConnection, csDB)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(sessionVars.SetSystemVar(vardef.CollationConnection, coDB))
}

func (e *SetExecutor) getVarValue(ctx context.Context, v *expression.VarAssignment, sysVar *variable.SysVar) (value string, err error) {
	if v.IsDefault {
		// To set a SESSION variable to the GLOBAL value or a GLOBAL value
		// to the compiled-in MySQL default value, use the DEFAULT keyword.
		// See http://dev.mysql.com/doc/refman/5.7/en/set-statement.html
		if sysVar != nil {
			defVal := variable.GlobalSystemVariableInitialValue(sysVar.Name, sysVar.Value)
			return defVal, nil
		}
		return e.Ctx().GetSessionVars().GetGlobalSystemVar(ctx, v.Name)
	}
	nativeVal, err := v.Expr.Eval(e.Ctx().GetExprCtx().GetEvalCtx(), chunk.Row{})
	if err != nil || nativeVal.IsNull() {
		return "", err
	}

	value, err = nativeVal.ToString()
	if err != nil {
		return "", err
	}

	// We need to clone the string because the value is constructed by `hack.String` in Datum which reuses the under layer `[]byte`
	// instead of allocating some new spaces. The `[]byte` in Datum will be reused in `chunk.Chunk` by different statements in session.
	// If we do not clone the value, the system variable will have a risk to be modified by other statements.
	return strings.Clone(value), nil
}

func (e *SetExecutor) loadSnapshotInfoSchemaIfNeeded(name string, snapshotTS uint64) error {
	if name != vardef.TiDBSnapshot && name != vardef.TiDBTxnReadTS {
		return nil
	}
	return loadSnapshotInfoSchemaIfNeeded(e.Ctx(), snapshotTS)
}

func loadSnapshotInfoSchemaIfNeeded(sctx sessionctx.Context, snapshotTS uint64) error {
	vars := sctx.GetSessionVars()
	if snapshotTS == 0 {
		vars.SnapshotInfoschema = nil
		return nil
	}
	logutil.BgLogger().Info("load snapshot info schema",
		zap.Uint64("conn", vars.ConnectionID),
		zap.Uint64("SnapshotTS", snapshotTS))
	dom := domain.GetDomain(sctx)
	snapInfo, err := dom.GetSnapshotInfoSchema(snapshotTS)
	if err != nil {
		return err
	}

	vars.SnapshotInfoschema = temptable.AttachLocalTemporaryTableInfoSchema(sctx, snapInfo)
	return nil
}

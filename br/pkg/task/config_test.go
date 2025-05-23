// Copyright 2024 PingCAP, Inc.
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

package task

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/metautil"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/statistics/util"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"google.golang.org/grpc/keepalive"
)

func TestRestoreConfigAdjust(t *testing.T) {
	cfg := &RestoreConfig{}
	cfg.Adjust()

	require.Equal(t, uint32(defaultRestoreConcurrency), cfg.Config.Concurrency)
	require.Equal(t, defaultSwitchInterval, cfg.Config.SwitchModeInterval)
	require.Equal(t, conn.DefaultMergeRegionKeyCount, cfg.MergeSmallRegionKeyCount.Value)
	require.Equal(t, conn.DefaultMergeRegionSizeBytes, cfg.MergeSmallRegionSizeBytes.Value)
}

type mockPDClient struct {
	pd.Client
}

func (m mockPDClient) GetClusterID(_ context.Context) uint64 {
	return 1
}

func (m mockPDClient) GetAllStores(ctx context.Context, opts ...opt.GetStoreOption) ([]*metapb.Store, error) {
	return []*metapb.Store{}, nil
}

func (m mockPDClient) WithCallerComponent(_ caller.Component) pd.Client {
	return m
}

func TestConfigureRestoreClient(t *testing.T) {
	cfg := Config{
		Concurrency: 1024,
	}
	restoreComCfg := RestoreCommonConfig{
		Online: true,
	}
	restoreCfg := &RestoreConfig{
		Config:              cfg,
		RestoreCommonConfig: restoreComCfg,
		DdlBatchSize:        128,
	}
	client := snapclient.NewRestoreClient(mockPDClient{}, nil, nil, keepalive.ClientParameters{})
	ctx := context.Background()
	err := configureRestoreClient(ctx, client, restoreCfg)
	require.NoError(t, err)
	require.Equal(t, uint(128), client.GetBatchDdlSize())
}

func TestAdjustRestoreConfigForStreamRestore(t *testing.T) {
	restoreCfg := RestoreConfig{}

	restoreCfg.adjustRestoreConfigForStreamRestore()
	require.Equal(t, restoreCfg.PitrBatchCount, uint32(defaultPiTRBatchCount))
	require.Equal(t, restoreCfg.PitrBatchSize, uint32(defaultPiTRBatchSize))
	require.Equal(t, restoreCfg.PitrConcurrency, uint32(defaultPiTRConcurrency)+1)
}

func TestCheckRestoreDBAndTable(t *testing.T) {
	cases := []struct {
		cfgSchemas map[string]struct{}
		cfgTables  map[string]struct{}
		backupDBs  map[string]*metautil.Database
	}{
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("test"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("test", "t"):  {},
				utils.EncloseDBAndTable("test", "t2"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"test": {"T", "T2"},
			}),
		},
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("mysql"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("mysql", "t"):  {},
				utils.EncloseDBAndTable("mysql", "t2"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"__TiDB_BR_Temporary_mysql": {"T", "T2"},
			}),
		},
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("test"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("test", "T"):  {},
				utils.EncloseDBAndTable("test", "T2"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"test": {"t", "t2"},
			}),
		},
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("TEST"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("TEST", "t"):  {},
				utils.EncloseDBAndTable("TEST", "T2"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"test": {"t", "t2"},
			}),
		},
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("TeSt"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("TeSt", "tabLe"):  {},
				utils.EncloseDBAndTable("TeSt", "taBle2"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"TesT": {"TablE", "taBle2"},
			}),
		},
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("TeSt"):  {},
				utils.EncloseName("MYSQL"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("TeSt", "tabLe"):  {},
				utils.EncloseDBAndTable("TeSt", "taBle2"): {},
				utils.EncloseDBAndTable("MYSQL", "taBle"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"TesT":                      {"table", "TaBLE2"},
				"__TiDB_BR_Temporary_mysql": {"tablE"},
			}),
		},
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("sys"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("sys", "t"):  {},
				utils.EncloseDBAndTable("sys", "t2"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"__TiDB_BR_Temporary_sys": {"T", "T2"},
			}),
		},
	}

	cfg := &RestoreConfig{}
	for _, ca := range cases {
		cfg.Schemas = ca.cfgSchemas
		cfg.Tables = ca.cfgTables

		backupDBs := make([]*metautil.Database, 0, len(ca.backupDBs))
		for _, db := range ca.backupDBs {
			backupDBs = append(backupDBs, db)
		}
		err := VerifyDBAndTableInBackup(backupDBs, cfg)
		require.NoError(t, err)
	}
}

func mockReadSchemasFromBackupMeta(t *testing.T, db2Tables map[string][]string) map[string]*metautil.Database {
	testDir := t.TempDir()
	store, err := storage.NewLocalStorage(testDir)
	require.NoError(t, err)

	mockSchemas := make([]*backuppb.Schema, 0)
	var dbID int64 = 1
	for db, tables := range db2Tables {
		dbName := ast.NewCIStr(db)
		mockTblList := make([]*model.TableInfo, 0)
		tblBytesList, statsBytesList := make([][]byte, 0), make([][]byte, 0)

		for i, table := range tables {
			tblName := ast.NewCIStr(table)
			mockTbl := &model.TableInfo{
				ID:   dbID*100 + int64(i),
				Name: tblName,
			}
			mockTblList = append(mockTblList, mockTbl)

			mockStats := util.JSONTable{
				DatabaseName: dbName.String(),
				TableName:    tblName.String(),
			}

			tblBytes, err := json.Marshal(mockTbl)
			require.NoError(t, err)
			tblBytesList = append(tblBytesList, tblBytes)

			statsBytes, err := json.Marshal(mockStats)
			require.NoError(t, err)
			statsBytesList = append(statsBytesList, statsBytes)
		}

		mockDB := model.DBInfo{
			ID:   dbID,
			Name: dbName,
		}
		mockDB.Deprecated.Tables = mockTblList
		dbID++
		dbBytes, err := json.Marshal(mockDB)
		require.NoError(t, err)

		for i := range tblBytesList {
			mockSchemas = append(mockSchemas, &backuppb.Schema{
				Db:    dbBytes,
				Table: tblBytesList[i],
				Stats: statsBytesList[i],
			},
			)
		}
	}

	mockFiles := []*backuppb.File{
		{
			Name:     fmt.Sprintf("%p.sst", &mockSchemas),
			StartKey: tablecodec.EncodeRowKey(1, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(2, []byte("a")),
		},
	}

	meta := mockBackupMeta(mockSchemas, mockFiles)
	data, err := proto.Marshal(meta)
	require.NoError(t, err)

	ctx := context.Background()
	err = store.WriteFile(ctx, metautil.MetaFile, data)
	require.NoError(t, err)

	dbs, err := metautil.LoadBackupTables(
		ctx,
		metautil.NewMetaReader(
			meta,
			store,
			&backuppb.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
			}),
		true,
	)
	require.NoError(t, err)
	return dbs
}

func mockBackupMeta(mockSchemas []*backuppb.Schema, mockFiles []*backuppb.File) *backuppb.BackupMeta {
	return &backuppb.BackupMeta{
		Files:   mockFiles,
		Schemas: mockSchemas,
	}
}

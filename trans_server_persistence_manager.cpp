#include "sqlite3.h"
#include "trans_server_persistence_manager.h"
#include "memory_management.h"
#include "protocol_subscribe_status_push.h"
#include "periodic_task_wrapper.h"
#include "trans_connection.h"
#include "trans_server_global.h"
#include <assert.h>
#include <vector>
#include <ITask.h>

inline void SetTrueIfFalse(bool& boolDest, bool boolSrc) 
{
	if (!boolDest)
	{
		boolDest = boolSrc;
	}
}

class TransServerSqlTransctionManagedTimerTask : public ITask
{
public:
	TransServerSqlTransctionManagedTimerTask(TransServerPersistenceManager &persistMgr)
		: persist_mgr_(persistMgr) {}
	virtual ~TransServerSqlTransctionManagedTimerTask(void){}
	virtual bool Execute(void)
	{
		int res = SQLITE_OK;
		TransServerPersistenceManager::ScopedErrorDescription description;
		return persist_mgr_.TryTransactionEnd(res, description.reset());
	}
private:
	TransServerPersistenceManager &persist_mgr_;
};

static __declspec(thread) vector< map<string, string> > *g_sql_record_storage_ = nullptr;

class ScopedSqlRecordStorageWrapper
{
private:
	ScopedSqlRecordStorageWrapper(const ScopedSqlRecordStorageWrapper&){}
	ScopedSqlRecordStorageWrapper& operator= (const ScopedSqlRecordStorageWrapper&){return *this;}
public:
	ScopedSqlRecordStorageWrapper(void) 
	{
		if (g_sql_record_storage_ != nullptr)
		{
			assert(false);
			throw std::exception("g_sql_record_storage_ allocation repeated.");
		}
		g_sql_record_storage_ = new vector< map<string, string> >;
	}
	~ScopedSqlRecordStorageWrapper(void) { ::SafeDeleteObject(g_sql_record_storage_);}
};

#define ALLOCATE_SCOPED_SQL_RECORD_STORAGE ScopedSqlRecordStorageWrapper __ssrs__;

char*& TransServerPersistenceManager::ScopedErrorDescription::reset(void)
{
	if (descrpiton_ != nullptr)
	{
		::sqlite3_free(descrpiton_);
		descrpiton_ = nullptr;
	}
	return descrpiton_;
}

TransServerPersistenceManager::TransServerPersistenceManager(void)
	: sqlite_handle_(nullptr), transaction_has_begun_(false), 
	rw_lock_(QReadWriteLock::Recursive), has_transaction_err_(false)
{
	::sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
	if (OpenSqliteDB())
	{
		if (CreateTables())
		{
			InitDataStatistics();
		}
		else
		{
			CloseSqliteDB();
		}
	}
}

TransServerPersistenceManager::~TransServerPersistenceManager(void)
{
	CloseSqliteDB();
}

int SqlExecuteCallback(void *notUsed, int argc, char **argv, char **azColName)
{
	if (g_sql_record_storage_ == nullptr)
	{
		return -1;
	}
	map<string, string> row;
	for (int i = 0; i < argc; i++)
	{
		row[ string(azColName[i]) ] = string(argv[i] ? argv[i] : "NULL");
	}
	g_sql_record_storage_->push_back(move(row));
	return 0;
}

bool TransServerPersistenceManager::OpenSqliteDB(void)
{
	if (::sqlite3_open(".\\Persistence\\TranServer.db", &sqlite_handle_)
		!= SQLITE_OK)
	{
		CloseSqliteDB();
		return false;
	}
	return true;
}

void TransServerPersistenceManager::CloseSqliteDB(void)
{
	if (sqlite_handle_ != nullptr)
	{
		::sqlite3_close(sqlite_handle_);
		sqlite_handle_ = nullptr;
	}
}

bool TransServerPersistenceManager::CreateTables(void)
{
	bool table01 = false;
	bool table02 = false;
	bool index01 = false;
	if (IsAvailable())
	{
		ScopedErrorDescription description;
		static const char *statment01 = "create table Trans_Server_Data_Statistics(wellboreId char(32) primary key, succeedCount integer not null, failCount integer not null);";
		int res = ::sqlite3_exec(sqlite_handle_, statment01, nullptr, 0, &description.reset());
		table01 = (res == SQLITE_OK || description.descrpiton().rfind("already exists") > 0);
		static const char *statment02 = "create table Trans_Server_Event_Record(wellboreId char(32) not null, level char(16) not null, description integer not null, time integer not null, auxiliary blob, primary key (wellboreId, description, time));";
		res = ::sqlite3_exec(sqlite_handle_, statment02, nullptr, 0, &description.reset());
		table02 = (res == SQLITE_OK || description.descrpiton().rfind("already exists") > 0);
		static const char *statment03 = "create index if not exists WellboreId_Time_Index_On_Event_Record On Trans_Server_Event_Record(wellboreId, time);";
		res = ::sqlite3_exec(sqlite_handle_, statment03, nullptr, 0, &description.reset());
		index01 = (res == SQLITE_OK);
	}
	return (table01 && table02 && index01);
}

bool TransServerPersistenceManager::InitDataStatistics(void)
{
	if (IsAvailable())
	{
		ScopedErrorDescription description;
		static const char *statment = "select * from Trans_Server_Data_Statistics;";
		ALLOCATE_SCOPED_SQL_RECORD_STORAGE
		int res = ::sqlite3_exec(sqlite_handle_, statment, SqlExecuteCallback, 0, &description.reset());
		if (res == SQLITE_OK)
		{
			QWriteLocker lock(&rw_lock_);
			for (auto iter = g_sql_record_storage_->begin(); iter != g_sql_record_storage_->end(); ++iter)
			{
				string wellboreid = (*iter)["wellboreId"];
				if (!wellboreid.empty())
				{
					__int64 succCount = ::_atoi64((*iter)["succeedCount"].c_str());
					__int64 failCount = ::_atoi64((*iter)["failCount"].c_str());
					wid_to_ds_tbl_[wellboreid] = ::make_pair(succCount, failCount);
				}
			}
		}
		return (res == SQLITE_OK);
	}
	return false;
}

bool TransServerPersistenceManager::UpdateDataStatistics(string wellboreId, unsigned __int64 succCountInc, unsigned __int64 failCountInc, TransServerSqlResult *result)
{
	assert(!wellboreId.empty());
	QWriteLocker lock(&rw_lock_);
	auto iter = wid_to_ds_tbl_.find(wellboreId);
	char statment[256] = {0};
	unsigned __int64 newSuccCnt = 0;
	unsigned __int64 newFailCnt = 0;
	if (iter != wid_to_ds_tbl_.end())
	{
		newSuccCnt = iter->second.first + succCountInc;
		newFailCnt = iter->second.second + failCountInc;
		::sprintf(statment, "update Trans_Server_Data_Statistics set succeedCount = '%I64d', failCount = '%I64d' where wellboreId = '%s';", newSuccCnt, newFailCnt, wellboreId.c_str());
	}
	else
	{
		newSuccCnt += succCountInc;
		newFailCnt += failCountInc;
		::sprintf(statment, "insert into Trans_Server_Data_Statistics values('%s', '%I64d', '%I64d');", wellboreId.c_str(), newSuccCnt, newFailCnt);
	}
	wid_to_ds_tbl_[wellboreId] = make_pair(newSuccCnt, newFailCnt);
	ScopedErrorDescription description;
	int res = SQLITE_OK;
	TryTransactionBegin(res, description.reset());
	res = ::sqlite3_exec(sqlite_handle_, statment, nullptr, 0, &description.reset());
	if (result != nullptr)
	{
		result->first = res;
		result->second = description.descrpiton();
	}
	return (res == SQLITE_OK);
}

bool TransServerPersistenceManager::GetDataStatistics(string wellboreId, unsigned __int64 &succCount, unsigned __int64 &failCount)
{
	QReadLocker lock(&rw_lock_);
	auto iter = wid_to_ds_tbl_.find(wellboreId);
	if (iter == wid_to_ds_tbl_.end())
	{
		return false;
	}
	succCount = iter->second.first;
	failCount = iter->second.second;
	return true;
}

bool TransServerPersistenceManager::UpdateEventRecord(string wellboreId, unsigned int desc, unsigned int timeSecond, unsigned int timeMillSecond, void *auxParam, unsigned int auxParamLen, TransServerSqlResult *result)
{
	assert(!wellboreId.empty());
	char statment[256] = {0};
	int res = SQLITE_OK;
	ScopedErrorDescription description;
	unsigned __int64 timeCalc = (unsigned __int64)timeSecond * 1000 + (unsigned __int64)timeMillSecond;
	if (auxParam != nullptr)
	{
		::sprintf(statment, "insert into Trans_Server_Event_Record values('%s', '%s', '%d', '%I64d', ?);", 
			wellboreId.c_str(),
			ProtocolSubscribeStatusPush::GetDescriptionLevelString(desc).c_str(), 
			desc, 
			timeCalc);
		sqlite3_stmt *stat = nullptr;
		do 
		{
			QWriteLocker lock(&rw_lock_);
			TryTransactionBegin(res, description.reset());
			::sqlite3_prepare(sqlite_handle_, statment, -1, &stat, 0);
			::sqlite3_bind_blob(stat, 1, auxParam, auxParamLen, NULL);
			res = ::sqlite3_step(stat);
			string strErrMsg = sqlite3_errmsg(sqlite_handle_);
			::sqlite3_finalize(stat);
		} while (0);
	}
	else
	{
		::sprintf(statment, "insert into Trans_Server_Event_Record(wellboreId, level, description, time) values('%s', '%s', '%d', '%I64d');", 
			wellboreId.c_str(),
			ProtocolSubscribeStatusPush::GetDescriptionLevelString(desc).c_str(), 
			desc,
			timeCalc);
		do
		{
			QWriteLocker lock(&rw_lock_);
			res = ::sqlite3_exec(sqlite_handle_, statment, nullptr, 0, &description.reset());
		} while (0);
	}
	if (result != nullptr)
	{
		result->first = res;
		result->second = description.descrpiton();
	}
	return (res == SQLITE_OK || res == SQLITE_DONE);
}

bool TransServerPersistenceManager::GetEventRecord(
	string wellboreId, 
	SStatusRecordQueryTimeCond begin,
	SStatusRecordQueryTimeCond end, 
	unsigned int maxReturn,
	SProtocolStatisticsQueryStatusResult& stsQueryRes,
	TransServerSqlResult *result)
{
	int res = SQLITE_OK;
	if (end.tag != ProtocolStatisticsQueryBoundaryNotSet && 
		begin.tag != ProtocolStatisticsQueryBoundaryNotSet &&
		end < begin)
	{
		//::swap(end, begin);
		auto tmp = end;
		end = begin;
		begin = tmp;
	}
	unsigned __int64 beginTimeCalc = (unsigned __int64)begin.second * 1000 + (unsigned __int64)begin.milliSec;
	unsigned __int64 endTimeCalc = (unsigned __int64)end.second * 1000 + (unsigned __int64)end.milliSec;
	bool beginNotSet = (begin.tag == ProtocolStatisticsQueryBoundaryNotSet);
	string timeCondStat;
	if (!beginNotSet)
	{
		char tmp[256] = {0};
		if (begin.tag == ProtocolStatisticsQueryBoundaryIncluded)
			::sprintf(tmp, " time >= %I64d", beginTimeCalc);
		else if (begin.tag == ProtocolStatisticsQueryBoundaryExcluded)
			::sprintf(tmp, " time > %I64d", beginTimeCalc);
		timeCondStat = tmp;
	}
	bool endNotSet = (end.tag == ProtocolStatisticsQueryBoundaryNotSet);
	if (!endNotSet)
	{
		if (!beginNotSet)
			timeCondStat += " and ";
		char tmp[256] = {0};
		if (end.tag == ProtocolStatisticsQueryBoundaryIncluded)
			::sprintf(tmp, " time <= %I64d", endTimeCalc);
		else if (end.tag == ProtocolStatisticsQueryBoundaryExcluded)
			::sprintf(tmp, " time < %I64d", endTimeCalc);
		timeCondStat += string(tmp);
	}
	char originStat[256] = {0};
	::sprintf(originStat, "select * from Trans_Server_Event_Record where wellboreId = '%s' ", wellboreId.c_str());
	string statement(originStat);
	if (!beginNotSet || !endNotSet)
		statement = statement + " and " + timeCondStat;
	statement += " order by time desc ";
	if (maxReturn > 0)
	{
		char limitStat[32] = {0};
		::sprintf(limitStat, " limit %d ", maxReturn);
		statement += string(limitStat);
	}
	sqlite3_stmt * stmt = nullptr;
	do
	{
		QReadLocker lock(&rw_lock_);
		::sqlite3_prepare(sqlite_handle_, statement.c_str(), -1, &stmt, 0);
		res = ::sqlite3_step(stmt);
		while (res == SQLITE_ROW)
		{
			string wellbore = (const char*)::sqlite3_column_text(stmt, 0);
			string level = (const char*)::sqlite3_column_text(stmt, 1);
			unsigned int desc = ::sqlite3_column_int(stmt, 2);
			unsigned __int64 timeCalc = ::sqlite3_column_int64(stmt, 3);
			int auxLen = ::sqlite3_column_bytes(stmt, 4);
			char *auxBuff = (char*)::sqlite3_column_blob(stmt, 4);
			if (auxBuff == nullptr)
				auxLen = 0;
			SProtocolSubscribeStatusPush *stsInfo = (SProtocolSubscribeStatusPush *)::malloc(sizeof(SProtocolSubscribeStatusPush) + auxLen);
			::memcpy_s(stsInfo->wellboreId, kWellboreIdLength, wellbore.c_str(), kWellboreIdLength);
			stsInfo->description = desc;
			stsInfo->timeSec = timeCalc / 1000;
			stsInfo->timeMilliSec = timeCalc % 1000;
			stsInfo->auxiliaryLength = auxLen;
			if (auxLen > 0)
				::memcpy_s(stsInfo->auxiliaryData, auxLen, auxBuff, auxLen);
			stsQueryRes.push_back(stsInfo);
			res = ::sqlite3_step(stmt);
		}
		::sqlite3_finalize(stmt);
	} while(0);
	if (result != nullptr)
	{
		result->first = res;
		result->second = "";
	}
	return (res == SQLITE_OK || res == SQLITE_DONE);
}

bool TransServerPersistenceManager::TryTransactionBegin(int& res, char*& errMsg)
{
	QWriteLocker locker(&rw_lock_);
	if (!transaction_has_begun_)
	{
		res = ::sqlite3_exec(sqlite_handle_, "begin transaction", 0, 0, &errMsg);
		if (res == SQLITE_OK)
		{
			transaction_has_begun_ = true;
			PeriodicTaskWrapper *wrapper = PeriodicTaskWrapper::Build(new TransServerSqlTransctionManagedTimerTask(*this),
				500, false, 1);
			wrapper->SetSyncNotify(false);
			wrapper->SetTaskUtilityNestedThread(TransConnection::dispatcher_task_manage_thread());
			wrapper->Schedule(*GlobalThreadPoolExecutor::GetInstance());
			return true;
		}
		return false;
	}
	return false;
}

bool TransServerPersistenceManager::TryTransactionEnd(int& res, char*& errMsg)
{
	QWriteLocker locker(&rw_lock_);
	if (transaction_has_begun_)
	{
		if (has_transaction_err_)
		{
			res = ::sqlite3_exec(sqlite_handle_, "rollback transaction", 0, 0, &errMsg);
			has_transaction_err_ = false;
		}
		else
			res = ::sqlite3_exec(sqlite_handle_, "commit transaction", 0, 0, &errMsg);
		if (res == SQLITE_OK)
		{
			transaction_has_begun_ = false;
			return true;
		}
		return false;
	}
	return false;
}
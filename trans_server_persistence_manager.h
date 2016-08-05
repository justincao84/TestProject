#ifndef _TRANS_SERVER_PERSISTENCE_MANAGER_H_
#define _TRANS_SERVER_PERSISTENCE_MANAGER_H_

typedef struct sqlite3 sqlite3;

#include <QReadWriteLock>
#include <string>
#include <map>
#include "protocol_statistics_query.h"
#include "protocol_statistics_query_feedback.h"

using namespace std;

typedef pair<int, string> TransServerSqlResult;

typedef map<string, pair<unsigned __int64, unsigned __int64> > WellboreId2DataStatistics;

typedef struct _SStatusRecordQueryTimeCond
{
	bool operator < (const struct _SStatusRecordQueryTimeCond& other)
	{
		return second < other.second || 
			((second == other.second) && (milliSec < other.milliSec));
	}
	EProtocolStatisticsQueryTimeBoundaryTag tag;
	unsigned int second;
	unsigned int milliSec;
} SStatusRecordQueryTimeCond;

class TransServerPersistenceManager
{
	friend class TransServerSqlTransctionManagedTimerTask;
public:
	TransServerPersistenceManager(void);
	~TransServerPersistenceManager(void);
public:
	class ScopedErrorDescription
	{
		friend class TransServerPersistenceManager;
		friend class TransServerSqlTransctionManagedTimerTask;
	private:
		ScopedErrorDescription(const ScopedErrorDescription&){}
		ScopedErrorDescription& operator = (const ScopedErrorDescription&){return *this;} 
	public:
		ScopedErrorDescription(void) : descrpiton_(nullptr) {}
		~ScopedErrorDescription(void) {reset();}
	public:
		string descrpiton(void) {return descrpiton_ != nullptr ? move(string(descrpiton_)) : move(string(""));}
	private:
		char*& reset(void);
	private:
		char *descrpiton_;
	};
public:
	bool	IsAvailable(void) {return sqlite_handle_ != nullptr;}
	bool	UpdateDataStatistics(string wellboreId, unsigned __int64 succCountInc, unsigned __int64 failCountInc, TransServerSqlResult *result = nullptr);
	bool	UpdateEventRecord(string wellboreId, unsigned int desc, unsigned int timeSecond, unsigned int timeMillSecond, void *auxParam = nullptr, unsigned int auxParamLen = 0, TransServerSqlResult *result = nullptr);
	bool	GetDataStatistics(string wellboreId, unsigned __int64 &succCount, unsigned __int64 &failCount);
	bool	GetEventRecord(	string wellboreId, 
							SStatusRecordQueryTimeCond begin,
							SStatusRecordQueryTimeCond end,
							unsigned int maxReturn,
							SProtocolStatisticsQueryStatusResult& stsQueryRes,
							TransServerSqlResult *result = nullptr);
private:
	bool	OpenSqliteDB(void);
	void	CloseSqliteDB(void);
	bool	CreateTables(void);
	bool	InitDataStatistics(void);
	bool	TryTransactionBegin(int &res, char*& errMsg);
	bool	TryTransactionEnd(int& res, char*& errMsg);
private:
	sqlite3 *sqlite_handle_;
	WellboreId2DataStatistics wid_to_ds_tbl_;
	mutable QReadWriteLock	rw_lock_;
	bool	transaction_has_begun_;
	bool	has_transaction_err_;
};

#endif
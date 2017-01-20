#include "HAClusterHelper.h"
#include "RakPeerInterface.h"
#include "BitStream.h"
#include "GetTime.h"
#include "DS_Queue.h"
#include "HACluster.h"
#include <ctime>
#include "RakSleep.h"

namespace RakNet
{
	//-------------------------------  State manager -------------------------------------------

	CPtr<CClusterConfig> CHaStateManager::LoadConfig()
	{
		return CClusterConfig::Deserialize( *m_ConfigBuffer );
	}

	void CHaStateManager::SaveConfig(const CClusterConfig& config)
	{
		m_ConfigBuffer = config.Serialize();
	}

	void CHaStateManager::SaveState(const CServerState& state)
	{
		m_StateBuffer = state.Serialize();
	}

	CPtr<CServerState> CHaStateManager::ReadState()
	{
		return CServerState::Deserialize( *m_StateBuffer );
	}

	CPtr<CLogStore> CHaStateManager::LoadLogStore()
	{
		return cs_new<CFileSystemLogStore>( m_StrLogStore, -1 );
	}

	int32 CHaStateManager::ServerId()
	{
		return (int32) RakNetGUID::ToUint32(rakPeerInterface->GetMyGUID());
	}

	void CHaStateManager::SystemExit(const int exit_code)
	{
		rakPeerInterface->Shutdown( 100, 0, PacketPriority::HIGH_PRIORITY );
	}

	//-------------------------------  State machine -------------------------------------------

	void CHaStateMachine::Commit(const ulong log_idx, CBuffer& data)
	{

	}

	void CHaStateMachine::PreCommit(const ulong log_idx, CBuffer& data)
	{

	}

	void CHaStateMachine::Rollback(const ulong log_idx, CBuffer& data)
	{

	}

	void CHaStateMachine::SaveSnapshotData(CSnapshot& s, const ulong offset, CBuffer& data)
	{

	}

	bool CHaStateMachine::ApplySnapshot(CSnapshot& s)
	{
		false;
	}

	int CHaStateMachine::ReadSnapshotData(CSnapshot& s, const ulong offset, CBuffer& data)
	{
		return 0;
	}

	CPtr<CSnapshot> CHaStateMachine::LastSnapshot()
	{
		return CPtr<CSnapshot>();
	}

	void CHaStateMachine::CreateSnapshot(CSnapshot& s, CAsyncResult<bool>::handler_type& when_done)
	{

	}

	//-------------------------------  Logger class -------------------------------------------
	CHaClusterFileLogger::CHaClusterFileLogger(CHaClusterLoggerManager& ClusterLoggerManager, const std::string& log_file, ELogLevel::Type eLevel)
		: m_LoggerManager(ClusterLoggerManager), m_LogLevel(eLevel), m_LogFile(log_file), m_LogBuffer(), m_Mutex() {}

	CHaClusterFileLogger::~CHaClusterFileLogger()
	{
		Flush();
		m_LogFile.flush();
		m_LogFile.close();
		m_LoggerManager.RemoveLogger(this);
	}

	void CHaClusterFileLogger::Debug(const std::string& log_line)
	{
		if (m_LogLevel <= 0)
			write_log("debug", log_line);
	}

	void CHaClusterFileLogger::Info(const std::string& log_line)
	{
		if (m_LogLevel <= 1)
			write_log("info ", log_line);
	}

	void CHaClusterFileLogger::Warning(const std::string& log_line)
	{
		if (m_LogLevel <= 2)
			write_log("warn ", log_line);
	}

	void CHaClusterFileLogger::Error(const std::string& log_line)
	{
		if (m_LogLevel <= 3)
			write_log("error", log_line);
	}

	void CHaClusterFileLogger::Flush()
	{
		DataStructures::Queue<std::string> mBackup;
		{
			std::lock_guard<std::mutex> guard(m_Mutex);
			if (m_LogBuffer.Size() > 0)
				m_LogBuffer.Swap(mBackup);
		}

		for (int iIndex = 0; iIndex < mBackup.Size(); iIndex++)
		{
			m_LogFile << mBackup[iIndex] << std::endl;
		}
		mBackup.Clear(_FILE_AND_LINE_);
	}

	void CHaClusterFileLogger::write_log(const std::string& level, const std::string& log_line)
	{
		std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
		int ms = (int)(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000);
		__time64_t now_c = std::chrono::system_clock::to_time_t(now);
		std::tm* tm = std::gmtime(&now_c);
		std::hash<std::thread::id> hasher;
		std::string line(sstrfmt("%d/%d/%d %d:%d:%d.%d\t[%d]\t%s\t").fmt(tm->tm_mon + 1, tm->tm_mday, tm->tm_year + 1900, tm->tm_hour, tm->tm_min, tm->tm_sec, ms, hasher(std::this_thread::get_id()), level.c_str()));
		line += log_line;
		{
			std::lock_guard<std::mutex> guard(m_Mutex);
			m_LogBuffer.Push(line, _FILE_AND_LINE_ );
		}
	}
	
	//-------------------------------  Logger manager -------------------------------------------

	CHaClusterLoggerManager::CHaClusterLoggerManager() : m_bHasRemovedLogger(false) {}
	
	CHaClusterLoggerManager::~CHaClusterLoggerManager()
	{
		Clear();
	}


	CHaClusterFileLogger* CHaClusterLoggerManager::LoggerAdd( const std::string& log_file, ELogLevel::Type eLevel )
	{
		CHaClusterFileLogger* pLogger = OP_NEW_3<CHaClusterFileLogger>( _FILE_AND_LINE_, *this, log_file, eLevel );
		m_aLoggers.Push( pLogger, _FILE_AND_LINE_ );
		return pLogger;
	}
	
	void CHaClusterLoggerManager::RemoveLogger(CHaClusterFileLogger* pLogger)
	{
		unsigned int uiIndex = m_aLoggers.GetIndexOf( pLogger );
		if ( uiIndex != MAX_UNSIGNED_LONG )
		{
			m_aLoggers.RemoveAtIndexFast( uiIndex );
			m_aRemovedLoggers.Push(pLogger, _FILE_AND_LINE_ );
			m_bHasRemovedLogger = true;
		}
	}

	void CHaClusterLoggerManager::Progress()
	{
		if ( !m_bHasRemovedLogger )
			return;
		for( int iIndex = 0; iIndex < m_aRemovedLoggers.Size(); iIndex++ )
			OP_DELETE<CHaClusterFileLogger>( m_aRemovedLoggers[iIndex], _FILE_AND_LINE_ );
		m_aRemovedLoggers.Clear( true,_FILE_AND_LINE_ );
	}
	
	void CHaClusterLoggerManager::Clear()
	{
		for( int iIndex = 0; iIndex < m_aLoggers.Size(); iIndex++ )
			OP_DELETE<CHaClusterFileLogger>( m_aLoggers[iIndex], _FILE_AND_LINE_ );
		m_aLoggers.Clear( true,_FILE_AND_LINE_ );
	}


	//-------------------------------  Sheduler -------------------------------------------
	void CHADelayedTaskScheduler::Schedule(CPtr<CDelayedTask>& pTask, int32 iMs)
	{
		m_aTasks.Push( OP_NEW_3<CHaDelayedTask>( _FILE_AND_LINE_,  pTask, iMs, std::bind(&CHADelayedTaskScheduler::FreeTask, this, std::placeholders::_1)), _FILE_AND_LINE_ );
	}

	void CHADelayedTaskScheduler::FreeTask(void* ptr)
	{
		CHaDelayedTask* pTask = static_cast<CHaDelayedTask*>(ptr);
		if (!pTask)
		{
			assert(false);
			return;
		}
		//pTask->m_eStatus = CHADelayedTaskScheduler::
	

	}

	void CHADelayedTaskScheduler::OnCancel(CPtr<CDelayedTask>& pTask)
	{
		CHaDelayedTask* pHaTask = static_cast<CHaDelayedTask*>(pTask->GetContext());
		if (pHaTask)
			pHaTask->Cancel();
	}

	void CHADelayedTaskScheduler::Clear()
	{
		for (int iIndex = 0; iIndex < m_aTasks.Size(); iIndex)
		{
			CHaDelayedTask* pTasks = m_aTasks.Get(iIndex);
			if (pTasks->m_Task)
			{
				pTasks->m_Task->Cancel();
				OP_DELETE<CHaDelayedTask>(pTasks, _FILE_AND_LINE_);
			}
		}
		m_aTasks.Clear(false, _FILE_AND_LINE_);
	}
	
	void CHADelayedTaskScheduler::Progress()
	{
		TimeMS msCurrentTime = GetTimeMS();
		for (int iIndex = 0; iIndex < m_aTasks.Size(); iIndex)
		{
			CHaDelayedTask* pTasks = m_aTasks.Get(iIndex);
			if (pTasks->m_eStatus == CHaDelayedTask::EStatus::eAssigned && pTasks->m_MsStart <= msCurrentTime)
			{
				pTasks->m_Task->Execute();
				pTasks->m_eStatus = CHaDelayedTask::EStatus::eExecuted;
			}
			else if (pTasks->m_eStatus == CHaDelayedTask::EStatus::eFinished || pTasks->m_eStatus == CHaDelayedTask::EStatus::eExecuted )
			{
				m_aGc.Push(pTasks, _FILE_AND_LINE_);
				m_aTasks.RemoveAtIndex(iIndex);
				iIndex--;
			}
		}

		GC();
		m_LastCheck = msCurrentTime;
	}

	void CHADelayedTaskScheduler::GC()
	{
		for (int iIndex = 0; iIndex < m_aGc.Size(); iIndex)
		{
			OP_DELETE<CHaDelayedTask>( m_aGc.Get(iIndex), _FILE_AND_LINE_);
		}
		m_aGc.Clear(true, _FILE_AND_LINE_);
	}

	CHADelayedTaskScheduler::CHADelayedTaskScheduler() : m_LastCheck(GetTimeMS())
	{

	}

	CHADelayedTaskScheduler::~CHADelayedTaskScheduler()
	{
		Clear();
	}

	// Serializer helpers

	void SerializeState( const CServerState& State, BitStream* pBitStream )
	{
		pBitStream->Write( State.GetTerm() );
		pBitStream->Write( State.GetCommitIndex() );
		pBitStream->Write( State.GetVotedFor() );
	}
	
	void DeSerializeState( CPtr<CServerState>& State, BitStream* pBitStream )
	{
		int iTemp;
		unsigned long ulTemp;
		pBitStream->Read( ulTemp );
		State->SetTerm( ulTemp );

		pBitStream->Read( ulTemp );
		State->SetCommitIndex( ulTemp );

		pBitStream->Read( iTemp );
		State->SetVotedFor( iTemp );
	}


}
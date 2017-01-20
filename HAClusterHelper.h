/*
 * High availibity cluser helper
 * 
 */

#ifndef __HA_CLUSTER_HELPER_H
#define __HA_CLUSTER_HELPER_H

#include "NativeFeatureIncludes.h"
#include "RakNetTypes.h"
#include "Export.h"
#include "PacketPriority.h"
#include "PluginInterface2.h"
#include "DS_List.h"
#include "RakNetTime.h"
#include "cornerstone/cornerstone.h"
#include "DS_Queue.h"
#include "GetTime.h"
#include "RakPeerInterface.h"


namespace RakNet {

	using namespace cornerstone;
	class CHaClusterInterface;
	class CHaClusterLoggerManager;

	class CHaStateManager : public CStateManager
	{
	public:
		virtual CPtr<CClusterConfig> LoadConfig() override;
		virtual void SaveConfig(const CClusterConfig& config) override;
		virtual void SaveState(const CServerState& state) override;
		virtual CPtr<CServerState> ReadState() override;
		virtual CPtr<CLogStore> LoadLogStore() override;
		virtual int32 ServerId() override;
		virtual void SystemExit(const int exit_code) override;
		void SetLogDir( const std::string& strLogDir ) { m_StrLogStore = strLogDir; }
		void SetPeerInterface(RakPeerInterface* pPeer) { rakPeerInterface = pPeer;  }
		CPtr<CBuffer> m_ConfigBuffer;
		CPtr<CBuffer> m_StateBuffer;
		RakPeerInterface* rakPeerInterface;
		std::string m_StrLogStore;
	};
	//-------------------------------  RpcClient -------------------------------------------

	class CHaStateMachine : public CStateMachine
	{
	public:
		virtual void Commit(const ulong log_idx, CBuffer& data) override;
		virtual void PreCommit(const ulong log_idx, CBuffer& data) override;
		virtual void Rollback(const ulong log_idx, CBuffer& data) override;
		virtual void SaveSnapshotData(CSnapshot& s, const ulong offset, CBuffer& data) override;
		virtual bool ApplySnapshot(CSnapshot& s) override;
		virtual int ReadSnapshotData(CSnapshot& s, const ulong offset, CBuffer& data) override;
		virtual CPtr<CSnapshot> LastSnapshot() override;
		virtual void CreateSnapshot(CSnapshot& s, CAsyncResult<bool>::handler_type& when_done)  override;
		void SetRakPeerInterface(RakPeerInterface* pPeer) { rakPeerInterface = pPeer;  }
		RakPeerInterface* rakPeerInterface;
	};

	//-------------------------------  RpcClient -------------------------------------------
	class CHaClient : public CRpcClient
	{
		public:
		RakPeerInterface* rakPeerInterface;
		CHaClusterInterface& m_Interface;

		CHaClient( RakPeerInterface* pPeer, const SystemAddress& SysAddr, CHaClusterInterface& Interface ) 
			: rakPeerInterface(pPeer)
			, m_SysAddr( SysAddr )
			, m_Interface( Interface )
		{
			// TODO mutex?
			Interface.m_aRpcClients.Push(this, _FILE_AND_LINE_ );
		}
		
		~CHaClient()
		{
			unsigned long ulIndex = m_Interface.m_aRpcClients.GetIndexOf( this );
			if ( ulIndex != MAX_UNSIGNED_LONG )
			{
				m_Interface.m_aRpcClients.RemoveAtIndex( ulIndex );
			}
		}

		void Send(CPtr<CRequestMessage>& req, rpc_handler& when_done) override { }
		SystemAddress m_SysAddr;

	};

	//-------------------------------  Logger class -------------------------------------------
	class CHaClusterFileLogger : public CLogger {
	public:
		CHaClusterFileLogger(CHaClusterLoggerManager& ClusterLoggerManager, const std::string& log_file, ELogLevel::Type eLevel);
		virtual ~CHaClusterFileLogger() override;
		__nocopy__(CHaClusterFileLogger)
		virtual void Debug(const std::string& log_line) override;
		virtual void Info(const std::string& log_line) override;
		virtual void Warning(const std::string& log_line) override;
		virtual void Error(const std::string& log_line) override;
		void Flush();
	private:
		void write_log(const std::string& level, const std::string& log_line);
		CHaClusterLoggerManager& m_LoggerManager;
		ELogLevel::Type m_LogLevel;
		std::ofstream m_LogFile;
		DataStructures::Queue<std::string> m_LogBuffer;
		std::mutex m_Mutex;
	};

	class CHaClusterLoggerManager
	{
	public:
		CHaClusterLoggerManager();
		~CHaClusterLoggerManager();

		CHaClusterFileLogger* LoggerAdd( const std::string& log_file, ELogLevel::Type eLevel );
		void RemoveLogger(CHaClusterFileLogger* pLogger);
		void Progress();
		void Clear();
	private:
		bool m_bHasRemovedLogger;
		DataStructures::List<CHaClusterFileLogger*> m_aLoggers;
		DataStructures::List<CHaClusterFileLogger*> m_aRemovedLoggers;
	};

	static  void SerializeState( const CServerState& State, BitStream* pBitStream );
	static  void DeSerializeState( CPtr<CServerState>& State, BitStream* pBitStream );

	//-------------------------------  Delayed task scheduler -------------------------------------------


	class RAK_DLL_EXPORT CHaDelayedTask
	{
	public:

		struct EStatus
		{
			enum Type
			{
				eAssigned,
				eExecuted,
				eFinished
			};
		};

		CHaDelayedTask() {}
		CHaDelayedTask(CPtr<CDelayedTask> pTask, int32 iMs, std::function<void(void*)> remove_task) : m_Task(pTask), m_iMs(iMs), m_eStatus(EStatus::eAssigned)
		{
			m_Task->SetContext(this, remove_task);
			m_MsStart = GetTimeMS() + m_iMs;
		}
		void Cancel() { m_eStatus = EStatus::eFinished; }

		inline bool operator < (const CHaDelayedTask& Task) { return Task.m_iMs > m_iMs; }
		inline bool operator > (const CHaDelayedTask& Task) { return Task.m_iMs < m_iMs; }
		inline bool operator == (const CHaDelayedTask& Taks) { return m_Task.get() == Taks.m_Task.get(); }
		
		int iTaskId;
		EStatus::Type m_eStatus;
		CPtr<CDelayedTask> m_Task;
		int32 m_iMs;
		TimeMS m_MsStart;
	};


	class CHADelayedTaskScheduler : public CDelayedTaskScheduler
	{

	public:
		CHADelayedTaskScheduler();
		~CHADelayedTaskScheduler();
		virtual void Schedule(CPtr<CDelayedTask>& pTask, int32 iMs) override;

		void Clear();
		void Progress();
	private:
		DataStructures::List<CHaDelayedTask*> m_aTasks;
		DataStructures::List<CHaDelayedTask*> m_aGc;
		TimeMS m_LastCheck;
		void FreeTask(void* ptr);
		virtual void OnCancel(CPtr<CDelayedTask>& pTask) override ;
		void GC();
	};

}

#endif


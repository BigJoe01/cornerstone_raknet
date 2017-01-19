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

namespace RakNet {

	using namespace cornerstone;
	class CHaClusterInterface;

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
		void RemoveLogger(CHaClusterFileLogger* pLogger);
	};

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


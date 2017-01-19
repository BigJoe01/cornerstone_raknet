#include "HACluster.h"
#include "RakPeerInterface.h"
#include "BitStream.h"
#include "GetTime.h"
#include "DS_Queue.h"
namespace RakNet
{

	CHaClusterInterface::CHaClusterInterface()
	{
	}

	CHaClusterInterface::~CHaClusterInterface()
	{

	}

	void CHaClusterInterface::OnAttach()
	{

	}

	void CHaClusterInterface::OnDetach()
	{

	}

	void CHaClusterInterface::OnRakPeerStartup()
	{
		m_LastCheck = GetTimeMS();
		m_MyGuid = rakPeerInterface->GetMyGUID();
	}

	void CHaClusterInterface::OnRakPeerShutdown()
	{

	}


	void CHaClusterInterface::Update()
	{

	}

	//---------------------------------- CORNERSTONE -----------------------------------------
	void CHaClusterInterface::Schedule(CPtr<CDelayedTask>& task, int32 milliseconds)
	{		
		m_aTasks.Push( OP_NEW_3<CHaTask>( _FILE_AND_LINE_, task, milliseconds, std::bind(&CHaClusterInterface::FreeTask, this, _1) ), _FILE_AND_LINE_ );
	}

	CPtr<CRpcClient> CHaClusterInterface::CreateClient(const std::string& endpoint)
	{
		return cs_new<CRpcClient>();
		//BIGJOE modositani kell ha implementalva lesz
	}

	void CHaClusterInterface::OnCancel(CPtr<CDelayedTask>& task)
	{
		if (task->GetContext() != nullptr )
			static_cast<CHaTask*>(task->GetContext())->Cancel();
	}

	CLogger* CHaClusterInterface::CreateLogger( ELogLevel::Type eLevel, const std::string& strLogFile )
	{
		CHaClusterFileLogger* pLogger = new CHaClusterFileLogger(*this, strLogFile, eLevel);
		m_aLoggers.Push(pLogger, _FILE_AND_LINE_);
		return pLogger ;
	}

}
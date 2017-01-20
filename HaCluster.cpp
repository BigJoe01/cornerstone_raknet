#include "HACluster.h"
#include "RakPeerInterface.h"
#include "BitStream.h"
#include "GetTime.h"
#include "DS_Queue.h"
#include "RakSleep.h"

namespace RakNet
{

	CHaClusterInterface::CHaClusterInterface() 
		: m_pRaftServer( nullptr)
		, m_pContext( nullptr ) { }

	CHaClusterInterface::~CHaClusterInterface()
	{

	}

	void CHaClusterInterface::OnAttach()
	{
		m_StateManager.SetPeerInterface(rakPeerInterface);
		m_StateMachine.SetPeerInterface(rakPeerInterface);
	}

	void CHaClusterInterface::OnDetach()
	{

	}

	void CHaClusterInterface::OnRakPeerStartup()
	{
		m_aRpcClients.Clear( false, _FILE_AND_LINE_ );
		m_LastCheck = GetTimeMS();
		m_MyGuid = rakPeerInterface->GetMyGUID();
		m_pContext = new SRaftContext( m_StateManager, m_StateMachine, *this, *m_LoggerManager.LoggerAdd( m_strLogFile, ELogLevel::eDebug ), *this, m_TaskScheduler, &m_RaftParams );
		m_pRaftServer =  OP_NEW_1<CRaftServer>( _FILE_AND_LINE_, m_pContext );
	}

	void CHaClusterInterface::OnRakPeerShutdown()
	{
		m_TaskScheduler.Clear();
		m_LoggerManager.Clear();
		if ( m_pRaftServer)
			OP_DELETE<CRaftServer>( m_pRaftServer, _FILE_AND_LINE_ );
		if ( m_pContext )
			OP_DELETE<SRaftContext>( m_pContext, _FILE_AND_LINE_ );
		
		m_aRpcClients.Clear( false, _FILE_AND_LINE_ );
	}


	void CHaClusterInterface::Update()
	{

	}

	//---------------------------------- CORNERSTONE CREATE NEW CLIENT -----------------------------------------
	
	

	CPtr<CRpcClient> CHaClusterInterface::CreateClient(const std::string& endpoint)
	{
		SystemAddress SysAddr;
		SysAddr.FromString( endpoint.c_str() );
		std::string strHost = SysAddr.ToString(false);
		unsigned short usPort = SysAddr.GetPort();
		rakPeerInterface->Connect( strHost.c_str(), usPort, nullptr, 0);
		return cs_new<CHaClient>(rakPeerInterface, SysAddr, *this );
	}

}
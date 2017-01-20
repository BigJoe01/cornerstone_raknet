/*
 * High availibity cluser
 * 
 */

#ifndef __HA_CLUSTER_INTERFACE_H
#define __HA_CLUSTER_INTERFACE_H

#include "NativeFeatureIncludes.h"
#include "RakNetTypes.h"
#include "Export.h"
#include "PacketPriority.h"
#include "PluginInterface2.h"
#include "DS_List.h"
#include "RakNetTime.h"
#include "HAClusterHelper.h"
#include "cornerstone/cornerstone.h"
#include "cs_fs_log_store.h"

namespace RakNet {
	
	using namespace cornerstone;
	using namespace std::placeholders;


	class CHaClient;

class CHaClusterFileLogger;

class RAK_DLL_EXPORT CHaClusterInterface
	: public PluginInterface2
	, public CRpcListener
	, public CRpcClientFactory
{
	friend CHaClient;
public:
	CHaClusterInterface( );
	virtual ~CHaClusterInterface();

	void SetLogStorePath( const std::string& strDir ) { m_StateManager.m_StrLogStore = strDir; }
	std::string GetLogStorePath() { return m_StateManager.m_StrLogStore; }
	CRaftParams& GetParams() { return m_RaftParams; }
	void SetLogFilePath( const std::string& strFile ) { m_strLogFile = strFile; }

	//-------------------------- CORNERSTONE IMP ----------------------------------------------
protected:

	virtual void OnAttach(void) override;
	virtual void OnDetach(void) override;
	virtual void Update(void) override;

	virtual PluginReceiveResult OnReceive(Packet *packet) { (void)packet; return RR_CONTINUE_PROCESSING; }
	virtual void OnRakPeerStartup(void);
	virtual void OnRakPeerShutdown(void);
	virtual void OnClosedConnection(const SystemAddress &systemAddress, RakNetGUID rakNetGUID, PI2_LostConnectionReason lostConnectionReason) { (void)systemAddress; (void)rakNetGUID; (void)lostConnectionReason; }
	virtual void OnNewConnection(const SystemAddress &systemAddress, RakNetGUID rakNetGUID, bool isIncoming) { (void)systemAddress; (void)rakNetGUID; (void)isIncoming; }
	virtual void OnFailedConnectionAttempt(Packet *packet, PI2_FailedConnectionAttemptReason failedConnectionAttemptReason) { (void)packet; (void)failedConnectionAttemptReason; }
	virtual bool UsesReliabilityLayer(void) const { return false; }
	virtual void OnDirectSocketSend(const char *data, const BitSize_t bitsUsed, SystemAddress remoteSystemAddress) { (void)data; (void)bitsUsed; (void)remoteSystemAddress; }
	virtual void OnDirectSocketReceive(const char *data, const BitSize_t bitsUsed, SystemAddress remoteSystemAddress) { (void)data; (void)bitsUsed; (void)remoteSystemAddress; }
	virtual void OnReliabilityLayerNotification(const char *errorMessage, const BitSize_t bitsUsed, SystemAddress remoteSystemAddress, bool isError) { (void)errorMessage; (void)bitsUsed; (void)remoteSystemAddress; (void)isError; }
	virtual void OnInternalPacket(InternalPacket *internalPacket, unsigned frameNumber, SystemAddress remoteSystemAddress, RakNet::TimeMS time, int isSend) { (void)internalPacket; (void)frameNumber; (void)remoteSystemAddress; (void)time; (void)isSend; }
	virtual void OnAck(unsigned int messageNumber, SystemAddress remoteSystemAddress, RakNet::TimeMS time) { (void)messageNumber; (void)remoteSystemAddress; (void)time; }
	virtual void OnPushBackPacket(const char *data, const BitSize_t bitsUsed, SystemAddress remoteSystemAddress) { (void)data; (void)bitsUsed; (void)remoteSystemAddress; }

	// RpcListener
	virtual void Listen(CPtr<msg_handler>& handler) override {}
	virtual void Stop() override {}


	// client factory
	virtual CPtr<CRpcClient> CreateClient(const std::string& endpoint) override;
	CHaStateManager m_StateManager;
	CHaStateMachine m_StateMachine;
	DataStructures::List<CHaClient*> m_aRpcClients;
	CHADelayedTaskScheduler m_TaskScheduler;
	CHaClusterLoggerManager m_LoggerManager;
	SRaftContext* m_pContext;
	CRaftServer* m_pRaftServer;
	CRaftParams m_RaftParams;

	TimeMS m_LastCheck;
	RakNetGUID m_MyGuid;
	std::string m_strLogFile;
};

}

#endif


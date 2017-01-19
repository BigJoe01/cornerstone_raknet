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
#include "cornerstone/cornerstone.h"
namespace RakNet {
	
	using namespace cornerstone;
	using namespace std::placeholders;

class CHaClusterFileLogger;



class RAK_DLL_EXPORT CHaClusterInterface : public PluginInterface2
{
public:
	CHaClusterInterface();
	virtual ~CHaClusterInterface();

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
	
	void FreeTask(void* ptr) {}
	TimeMS m_LastCheck;
	RakNetGUID m_MyGuid;
};

}

#endif


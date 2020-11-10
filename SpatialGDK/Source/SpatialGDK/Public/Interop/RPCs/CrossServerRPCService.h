// Copyright (c) Improbable Worlds Ltd, All Rights Reserved

#pragma once

#include "CoreMinimal.h"

#include "Interop/SpatialClassInfoManager.h"
#include "RPCStore.h"
#include "Schema/CrossServerEndpoint.h"
#include "Schema/RPCPayload.h"
#include "SpatialView/SubView.h"
#include "Utils/CrossServerUtils.h"
#include "Utils/RPCRingBuffer.h"

DECLARE_LOG_CATEGORY_EXTERN(LogCrossServerRPCService, Log, All);

class USpatialLatencyTracer;
class USpatialStaticComponentView;
class USpatialNetDriver;
struct RPCRingBuffer;

namespace SpatialGDK
{
struct FRPCStore;

struct CrossServerEndpoints
{
	// CrossServerEndpointSender
	CrossServerEndpointSenderACK ACKedRPCs;
	CrossServerEndpointReceiver ReceivedRPCs;
	// CrossServerEndpointReceiverACK
};

class SPATIALGDK_API CrossServerRPCService
{
public:
	CrossServerRPCService(const ExtractRPCDelegate InExtractRPCCallback, const FSubView& InSubView, USpatialNetDriver* InNetDriver,
						  FRPCStore& InRPCStore);

	void Advance();

	// Public state functions for the main Spatial RPC service to expose bookkeeping around overflows and acks.
	// Could be moved into RPCStore. Note: Needs revisiting at some point, this is a strange boundary.
	// bool ContainsOverflowedRPC(const EntityRPCType& EntityRPC) const;
	// TMap<EntityRPCType, TArray<PendingRPCPayload>>& GetOverflowedRPCs();
	// void AddOverflowedRPC(EntityRPCType EntityType, PendingRPCPayload&& Payload);

	EPushRPCResult PushCrossServerRPC(Worker_EntityId EntityId, const RPCSender& Sender, const PendingRPCPayload& Payload,
									  bool bCreatedEntity);

	void WriteCrossServerACKFor(Worker_EntityId Receiver, const RPCSender& Sender);
	void FlushPendingClearedFields();

private:
	// Process relevant view delta changes.
	void EntityAdded(const Worker_EntityId EntityId);
	void ComponentUpdate(const Worker_EntityId EntityId, const Worker_ComponentId ComponentId, Schema_ComponentUpdate* Update);

	// Maintain local state of client server RPCs.
	void PopulateDataStore(Worker_EntityId EntityId);
	// void ApplyComponentUpdate(Worker_EntityId EntityId, Worker_ComponentId ComponentId, Schema_ComponentUpdate* Update);

	// Client server RPC system responses to state changes.
	void OnEndpointAuthorityGained(Worker_EntityId EntityId, Worker_ComponentId ComponentId);
	void OnEndpointAuthorityLost(Worker_EntityId EntityId, Worker_ComponentId ComponentId);
	// void ClearOverflowedRPCs(Worker_EntityId EntityId);

	// The component with the given component ID was updated, and so there is an RPC to be handled.
	void HandleRPC(const Worker_EntityId EntityId, const CrossServerEndpointReceiver&);
	//// Calls ExtractRPCCallback for each RPC it extracts from a given component. If the callback returns false,
	//// stops retrieving RPCs.
	void ExtractCrossServerRPCs(Worker_EntityId EntityId, const CrossServerEndpointReceiver&);
	void UpdateSentRPCsACKs(Worker_EntityId, const CrossServerEndpointSenderACK&);
	void CleanupACKsFor(Worker_EntityId EndpointId, const CrossServerEndpointReceiver&);
	//
	//// Helpers
	static bool IsCrossServerEndpoint(Worker_ComponentId ComponentId);

	ExtractRPCDelegate ExtractRPCCallback;
	const FSubView* SubView;
	USpatialNetDriver* NetDriver;

	FRPCStore* RPCStore;

	// Deserialized state store for client/server RPC components.
	TMap<Worker_EntityId_Key, CrossServerEndpoints> CrossServerDataStore;

	// Stored here for things we have authority over.

	// For each sender actor, map of the sent RPCs.
	TMap<Worker_EntityId_Key, CrossServer::SenderState> ActorSenderState;

	// For receiver
	// Contains the number of available slots for acks for the given receiver.
	TMap<Worker_EntityId_Key, CrossServer::SlotAlloc> ACKAllocMap;

	struct ReceiverState
	{
		CrossServer::RPCSchedule Schedule;
		struct Item
		{
			uint32 Slot;
			CrossServer::ACKSlot ACKSlot;
		};
		TMap<CrossServer::RPCKey, Item> Slots;
	};
	// Map from receiving endpoint to ack slots.
	TMap<Worker_EntityId_Key, ReceiverState> ReceiverMap;

	TSet<Worker_EntityId_Key> StaleEntities;
};

} // namespace SpatialGDK

// Copyright (c) Improbable Worlds Ltd, All Rights Reserved

#include "Interop/RPCs/CrossServerRPCService.h"

#include "EngineClasses/SpatialPackageMapClient.h"
#include "SpatialConstants.h"
#include "Utils/RepLayoutUtils.h"

DEFINE_LOG_CATEGORY(LogCrossServerRPCService);

namespace SpatialGDK
{
CrossServerRPCService::CrossServerRPCService(const ExtractRPCDelegate InExtractRPCCallback, const FSubView& InSubView,
											 USpatialNetDriver* InNetDriver, FRPCStore& InRPCStore)
	: ExtractRPCCallback(InExtractRPCCallback)
	, SubView(&InSubView)
	, NetDriver(InNetDriver)
	, RPCStore(&InRPCStore)
{
}

EPushRPCResult CrossServerRPCService::PushCrossServerRPC(Worker_EntityId EntityId, const RPCSender& Sender,
														 const PendingRPCPayload& Payload, bool bCreatedEntity)
{
	CrossServerEndpoints* Endpoints = CrossServerDataStore.Find(Sender.Entity);
	Schema_Object* EndpointObject = nullptr;
	EntityComponentId SenderEndpointId(Sender.Entity, SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID);

	if (!Endpoints)
	{
		if (bCreatedEntity)
		{
			return EPushRPCResult::EntityBeingCreated;
		}

		EndpointObject = Schema_GetComponentDataFields(RPCStore->GetOrCreateComponentData(SenderEndpointId));
	}
	else
	{
		EndpointObject = Schema_GetComponentUpdateFields(RPCStore->GetOrCreateComponentUpdate(SenderEndpointId, nullptr));
	}

	CrossServer::SenderState& SenderState = ActorSenderState.FindOrAdd(Sender.Entity);

	TOptional<uint32> Slot = SenderState.Alloc.PeekFreeSlot();
	if (!Slot)
	{
		if (RPCRingBufferUtils::ShouldQueueOverflowed(ERPCType::CrossServerSender))
		{
			return EPushRPCResult::QueueOverflowed;
		}
		else
		{
			return EPushRPCResult::DropOverflowed;
		}
	}

	uint64 NewRPCId = SenderState.LastSentRPCId++;
	uint32 SlotIdx = Slot.GetValue();

	RPCRingBufferDescriptor Descriptor = RPCRingBufferUtils::GetRingBufferDescriptor(ERPCType::CrossServerSender);
	uint32 Field = Descriptor.GetRingBufferElementFieldId(ERPCType::CrossServerSender, SlotIdx + 1);

	Schema_Object* RPCObject = Schema_AddObject(EndpointObject, Field);

	RPCTarget Target(CrossServerRPCInfo(EntityId, NewRPCId));
	CrossServer::WritePayloadAndCounterpart(EndpointObject, Payload.Payload, Target, SlotIdx);

	Schema_ClearField(EndpointObject, Descriptor.LastSentRPCFieldId);
	Schema_AddUint64(EndpointObject, Descriptor.LastSentRPCFieldId, SenderState.LastSentRPCId);

	CrossServer::RPCKey RPCKey(Sender.Entity, NewRPCId);
	CrossServer::SentRPCEntry Entry;
	// Entry.RPCId = RPCKey;
	Entry.Target = Target;
	// Entry.Timestamp = FPlatformTime::Cycles64();
	Entry.SourceSlot = SlotIdx;

	SenderState.Mailbox.Add(RPCKey, Entry);
	SenderState.Alloc.CommitSlot(SlotIdx);

	return EPushRPCResult::Success;
}

void CrossServerRPCService::Advance()
{
	const FSubViewDelta& SubViewDelta = SubView->GetViewDelta();
	for (const EntityDelta& Delta : SubViewDelta.EntityDeltas)
	{
		switch (Delta.Type)
		{
		case EntityDelta::UPDATE:
		{
			for (const ComponentChange& Change : Delta.ComponentUpdates)
			{
				ComponentUpdate(Delta.EntityId, Change.ComponentId, Change.Update);
			}
			break;
		}
		case EntityDelta::ADD:
			PopulateDataStore(Delta.EntityId);
			EntityAdded(Delta.EntityId);
			break;
		case EntityDelta::REMOVE:
			CrossServerDataStore.Remove(Delta.EntityId);
			break;
		case EntityDelta::TEMPORARILY_REMOVED:
			CrossServerDataStore.Remove(Delta.EntityId);
			PopulateDataStore(Delta.EntityId);
			EntityAdded(Delta.EntityId);
			break;
		default:
			break;
		}
	}
}

// bool CrossServerRPCService::ContainsOverflowedRPC(const EntityRPCType& EntityRPC) const
//{
//	return OverflowedRPCs.Contains(EntityRPC);
//}
//
// TMap<EntityRPCType, TArray<PendingRPCPayload>>& CrossServerRPCService::GetOverflowedRPCs()
//{
//	return OverflowedRPCs;
//}
//
// void CrossServerRPCService::AddOverflowedRPC(const EntityRPCType EntityType, PendingRPCPayload&& Payload)
//{
//	OverflowedRPCs.FindOrAdd(EntityType).Add(MoveTemp(Payload));
//}

void CrossServerRPCService::EntityAdded(const Worker_EntityId EntityId)
{
	for (const Worker_ComponentId ComponentId : SubView->GetView()[EntityId].Authority)
	{
		if (!IsCrossServerEndpoint(ComponentId))
		{
			continue;
		}
		OnEndpointAuthorityGained(EntityId, ComponentId);
	}
	// Look at the stale entities ? Is it even needed anymore to delay it ?
	// CrossServerEndpoints* Endpoints = CrossServerDataStore.Find(EntityId);
	// ExtractCrossServerRPCs(EntityId, Endpoints->ReceivedRPCs);
}

void CrossServerRPCService::ComponentUpdate(const Worker_EntityId EntityId, const Worker_ComponentId ComponentId,
											Schema_ComponentUpdate* Update)
{
	if (!IsCrossServerEndpoint(ComponentId))
	{
		return;
	}

	if (CrossServerEndpoints* Endpoints = CrossServerDataStore.Find(EntityId))
	{
		switch (ComponentId)
		{
		case SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID:
			Endpoints->ReceivedRPCs.ApplyComponentUpdate(Update);
			HandleRPC(EntityId, Endpoints->ReceivedRPCs);
			break;

		case SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID:
			Endpoints->ACKedRPCs.ApplyComponentUpdate(Update);
			UpdateSentRPCsACKs(EntityId, Endpoints->ACKedRPCs);
			break;
		default:
			break;
		}
	}
}

void CrossServerRPCService::PopulateDataStore(const Worker_EntityId EntityId)
{
	const EntityViewElement& Entity = SubView->GetView()[EntityId];

	Schema_ComponentData* SenderACKData =
		Entity.Components.FindByPredicate(ComponentIdEquality{ SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID })
			->GetUnderlying();
	Schema_ComponentData* ReceiverData =
		Entity.Components.FindByPredicate(ComponentIdEquality{ SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID })
			->GetUnderlying();

	CrossServerDataStore.Emplace(
		EntityId, CrossServerEndpoints{ CrossServerEndpointSenderACK(SenderACKData), CrossServerEndpointReceiver(ReceiverData) });
}

void CrossServerRPCService::OnEndpointAuthorityGained(const Worker_EntityId EntityId, const Worker_ComponentId ComponentId)
{
	const EntityViewElement& Entity = SubView->GetView()[EntityId];
	const ComponentData* Data = Entity.Components.FindByPredicate([&](ComponentData& Data) {
		return Data.GetComponentId() == ComponentId;
	});

	check(Data != nullptr);

	switch (ComponentId)
	{
	case SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID:
	{
		CrossServerEndpointSender SenderEndpoint(Data->GetUnderlying());
		CrossServer::SenderState& SenderState = ActorSenderState.FindOrAdd(EntityId);
		SenderState.LastSentRPCId = SenderEndpoint.ReliableRPCBuffer.LastSentRPCId;
		for (int32 SlotIdx = 0; SlotIdx < SenderEndpoint.ReliableRPCBuffer.RingBuffer.Num(); ++SlotIdx)
		{
			const auto& Slot = SenderEndpoint.ReliableRPCBuffer.RingBuffer[SlotIdx];
			if (Slot.IsSet())
			{
				const TOptional<CrossServerRPCInfo>& TargetRef = SenderEndpoint.ReliableRPCBuffer.Counterpart[SlotIdx];
				check(TargetRef.IsSet());

				CrossServer::RPCKey RPCKey(EntityId, TargetRef.GetValue().RPCId);

				CrossServer::SentRPCEntry NewEntry;
				// NewEntry.RPCId = RPCKey;
				NewEntry.Target = RPCTarget(TargetRef.GetValue());
				NewEntry.SourceSlot = SlotIdx;
				// NewEntry.Timestamp = FPlatformTime::Cycles64();
				// NewEntry.EntityRequest = 0;

				SenderState.Mailbox.Add(RPCKey, NewEntry);
				SenderState.Alloc.Occupied[SlotIdx] = true;
			}
		}
		StaleEntities.Add(EntityId);

		break;
	}
	case SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID:
	{
		CrossServerEndpointReceiverACK ReceiverACKEndpoint(Data->GetUnderlying());

		uint32 numAcks = 0;
		for (int32 SlotIdx = 0; SlotIdx < ReceiverACKEndpoint.ACKArray.Num(); ++SlotIdx)
		{
			const TOptional<ACKItem>& ACK = ReceiverACKEndpoint.ACKArray[SlotIdx];
			if (ACK)
			{
				CrossServer::ACKSlot NewSlot;
				NewSlot.Receiver = EntityId;
				NewSlot.Slot = SlotIdx;
				Worker_EntityId ReceivingEndpoint = EntityId;
				ReceiverState& Receiver = ReceiverMap.FindOrAdd(ReceivingEndpoint);
				Receiver.Slots.Add(CrossServer::RPCKey(ACK->Sender, ACK->RPCId), ReceiverState::Item()).ACKSlot = NewSlot;

				check(ReceivingEndpoint == NewSlot.Receiver);

				CrossServer::SlotAlloc& ACKAlloc = ACKAllocMap.FindOrAdd(EntityId);
				ACKAlloc.Occupied[SlotIdx] = true;
			}
		}

		StaleEntities.Add(EntityId);

		break;
	}
	default:
		checkNoEntry();
		break;
	}
}

void CrossServerRPCService::OnEndpointAuthorityLost(const Worker_EntityId EntityId, const Worker_ComponentId ComponentId)
{
	switch (ComponentId)
	{
	case SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID:
		ActorSenderState.Remove(EntityId);
		break;
	case SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID:
		ACKAllocMap.Remove(EntityId);
		ReceiverMap.Remove(EntityId);
		break;
	default:
		checkNoEntry();
		break;
	}
}

// void CrossServerRPCService::ClearOverflowedRPCs(const Worker_EntityId EntityId)
//{
//	for (uint8 RPCType = static_cast<uint8>(ERPCType::ClientReliable); RPCType <= static_cast<uint8>(ERPCType::NetMulticast); RPCType++)
//	{
//		OverflowedRPCs.Remove(EntityRPCType(EntityId, static_cast<ERPCType>(RPCType)));
//	}
//}

void CrossServerRPCService::HandleRPC(const Worker_EntityId EntityId, const CrossServerEndpointReceiver& Receiver)
{
	// When migrating an Actor to another worker, we preemptively change the role to SimulatedProxy when updating authority intent.
	// This can happen while this worker still has ServerEndpoint authority, and attempting to process a server RPC causes the engine
	// to print errors if the role isn't Authority. Instead, we exit here, and the RPC will be processed by the server that receives
	// authority.
	if (SubView->HasAuthority(EntityId, SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID))
	{
		const TWeakObjectPtr<UObject> ActorReceivingRPC = NetDriver->PackageMap->GetObjectFromEntityId(EntityId);
		if (!ActorReceivingRPC.IsValid())
		{
			UE_LOG(LogCrossServerRPCService, Log,
				   TEXT("Entity receiving ring buffer RPC does not exist in PackageMap, possibly due to corresponding actor getting "
						"destroyed. Entity: %lld, Component: %d"),
				   EntityId, SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID);
			return;
		}

		const bool bActorRoleIsSimulatedProxy = Cast<AActor>(ActorReceivingRPC.Get())->Role == ROLE_SimulatedProxy;
		if (bActorRoleIsSimulatedProxy)
		{
			UE_LOG(LogCrossServerRPCService, Verbose,
				   TEXT("Will not process server RPC, Actor role changed to SimulatedProxy. This happens on migration. Entity: %lld"),
				   EntityId);
			return;
		}
	}
	ExtractCrossServerRPCs(EntityId, Receiver);
}

bool CrossServerRPCService::IsCrossServerEndpoint(const Worker_ComponentId ComponentId)
{
	return ComponentId == SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID
		   || ComponentId == SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID
		   || ComponentId == SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID
		   || ComponentId == SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID;
}

void CrossServerRPCService::ExtractCrossServerRPCs(Worker_EntityId EndpointId, const CrossServerEndpointReceiver& Receiver)
{
	// First, try to free ACK slots.
	CleanupACKsFor(EndpointId, Receiver);

	const RPCRingBuffer& Buffer = Receiver.ReliableRPCBuffer;

	auto GetRPCInfo = [EndpointId](const TOptional<CrossServerRPCInfo>& Counterpart, RPCSender& SenderRef, Worker_EntityId& TargetId,
								   CrossServer::RPCKey& RPCKey) {
		Worker_EntityId CounterpartId = Counterpart.GetValue().Entity;
		uint64 RPCId = Counterpart.GetValue().RPCId;
		SenderRef = RPCSender(CrossServerRPCInfo(CounterpartId, RPCId));
		TargetId = EndpointId;
		RPCKey = CrossServer::RPCKey(SenderRef.Entity, RPCId);
	};

	auto* EndpointState = ReceiverMap.Find(EndpointId);

	for (uint32 Slot = 0; Slot < RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerReceiver); ++Slot)
	{
		const TOptional<RPCPayload>& Element = Buffer.RingBuffer[Slot];
		if (Element.IsSet())
		{
			const TOptional<CrossServerRPCInfo>& Counterpart = Buffer.Counterpart[Slot];
			if (ensure(Counterpart.IsSet()))
			{
				RPCSender SenderRef;
				Worker_EntityId TargetId;
				CrossServer::RPCKey RPCKey;
				GetRPCInfo(Counterpart, SenderRef, TargetId, RPCKey);

				const bool bAlreadyQueued = EndpointState != nullptr && EndpointState->Slots.Find(RPCKey) != nullptr;

				if (!bAlreadyQueued)
				{
					EndpointState = &ReceiverMap.FindOrAdd(EndpointId);
					auto& NewSlot = EndpointState->Slots.Add(RPCKey, ReceiverState::Item());
					NewSlot.Slot = Slot;
					NewSlot.ACKSlot.Receiver = TargetId;
					EndpointState->Schedule.Add(RPCKey);
				}
			}
		}
	}

	if (EndpointState == nullptr)
	{
		return;
	}

	while (!EndpointState->Schedule.IsEmpty())
	{
		CrossServer::RPCKey RPC = EndpointState->Schedule.Peek();
		ReceiverState::Item& Slots = EndpointState->Slots.FindChecked(RPC);

		const TOptional<CrossServerRPCInfo>& Counterpart = Buffer.Counterpart[Slots.Slot];
		RPCSender SenderRef;
		Worker_EntityId TargetId;
		CrossServer::RPCKey RPCKey;
		GetRPCInfo(Counterpart, SenderRef, TargetId, RPCKey);
		check(RPC == RPCKey);

		CrossServer::SlotAlloc* AvailableACKSlots = ACKAllocMap.Find(TargetId);
		bool bHasACKSlotsAvailable = (AvailableACKSlots == nullptr || AvailableACKSlots->Occupied.Find(false) >= 0);

		check(bHasACKSlotsAvailable);

		EndpointState->Schedule.Extract();

		const RPCPayload& Payload = Buffer.RingBuffer[Slots.Slot].GetValue();

		// const bool bKeepExtracting =
		ExtractRPCCallback.Execute(FUnrealObjectRef(TargetId, Payload.Offset), SenderRef, Payload);
		// if (!bKeepExtracting)
		//{
		//	break;
		//}
	}
}

void CrossServerRPCService::WriteCrossServerACKFor(Worker_EntityId Receiver, const RPCSender& Sender)
{
	CrossServer::SlotAlloc& AvailableACKSlots = ACKAllocMap.FindOrAdd(Receiver);
	int32 SlotIdx = AvailableACKSlots.Occupied.FindAndSetFirstZeroBit();
	check(SlotIdx >= 0);
	AvailableACKSlots.ToClear[SlotIdx] = false;

	ACKItem ACK;
	ACK.RPCId = Sender.RPCId;
	ACK.Sender = Sender.Entity;

	EntityComponentId Pair(Receiver, SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID);

	Schema_ComponentUpdate* Update = RPCStore->GetOrCreateComponentUpdate(Pair, nullptr);
	Schema_Object* UpdateObject = Schema_GetComponentUpdateFields(Update);
	// Schema_AddUint64(UpdateObject, 1, ACKComponent->RPCAck);
	Schema_Object* NewEntry = Schema_AddObject(UpdateObject, 2 + SlotIdx);
	ACK.WriteToSchema(NewEntry);

	ReceiverState& SlotsForSender = ReceiverMap.FindChecked(Receiver);

	CrossServer::ACKSlot& OccupiedSlot = SlotsForSender.Slots.FindChecked(CrossServer::RPCKey(Sender.Entity, Sender.RPCId)).ACKSlot;
	OccupiedSlot.Slot = SlotIdx;
}

void CrossServerRPCService::UpdateSentRPCsACKs(Worker_EntityId SenderId, const CrossServerEndpointSenderACK& ACKComponent)
{
	for (int32 SlotIdx = 0; SlotIdx < ACKComponent.ACKArray.Num(); ++SlotIdx)
	{
		if (ACKComponent.ACKArray[SlotIdx])
		{
			const ACKItem& ACK = ACKComponent.ACKArray[SlotIdx].GetValue();

			CrossServer::RPCKey RPCKey(ACK.Sender, ACK.RPCId);

			CrossServer::SenderState& SenderState = ActorSenderState.FindOrAdd(ACK.Sender);
			CrossServer::SentRPCEntry* SentRPC = SenderState.Mailbox.Find(RPCKey);
			if (SentRPC != nullptr)
			{
				SenderState.Alloc.Occupied[SentRPC->SourceSlot] = false;
				SenderState.Alloc.ToClear[SentRPC->SourceSlot] = true;
				SenderState.Mailbox.Remove(RPCKey);

				EntityComponentId Pair(ACK.Sender, SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID);
				RPCStore->GetOrCreateComponentUpdate(Pair, nullptr);
			}
		}
	}
}

void CrossServerRPCService::CleanupACKsFor(Worker_EntityId Endpoint, const CrossServerEndpointReceiver& Receiver)
{
	ReceiverState* State = ReceiverMap.Find(Endpoint);

	if (State != nullptr && State->Slots.Num() > 0)
	{
		TMap<CrossServer::RPCKey, ReceiverState::Item> ACKSToClear = State->Slots;
		for (auto Iterator = ACKSToClear.CreateIterator(); Iterator; ++Iterator)
		{
			if (Iterator->Value.ACKSlot.Slot == -1)
			{
				Iterator.RemoveCurrent();
			}
		}

		if (ACKSToClear.Num() == 0)
		{
			return;
		}

		const RPCRingBuffer& Buffer = Receiver.ReliableRPCBuffer;

		for (uint32 Slot = 0; Slot < RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerReceiver); ++Slot)
		{
			const TOptional<RPCPayload>& Element = Buffer.RingBuffer[Slot];
			if (Element.IsSet())
			{
				const TOptional<CrossServerRPCInfo>& Counterpart = Buffer.Counterpart[Slot];
				Worker_EntityId CounterpartId = Counterpart.GetValue().Entity;
				uint64 RPCId = Counterpart.GetValue().RPCId;

				CrossServer::RPCKey RPCKey(CounterpartId, RPCId);
				ACKSToClear.Remove(RPCKey);
			}
		}

		EntityComponentId Pair(0, SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID);

		for (auto const& SlotToClear : ACKSToClear)
		{
			Pair.EntityId = SlotToClear.Value.ACKSlot.Receiver;
			uint32 SlotIdx = SlotToClear.Value.ACKSlot.Slot;
			State->Slots.Remove(SlotToClear.Key);

			RPCStore->GetOrCreateComponentUpdate(Pair, nullptr);

			CrossServer::SlotAlloc& Slots = ACKAllocMap.FindChecked(Pair.EntityId);
			Slots.FreeSlot(SlotIdx);

			if (State->Slots.Num() == 0 && State->Schedule.IsEmpty())
			{
				ReceiverMap.Remove(Endpoint);
			}
		}
	}
}

void CrossServerRPCService::FlushPendingClearedFields()
{
	for (auto& UpdateToSend : RPCStore->PendingComponentUpdatesToSend)
	{
		if (UpdateToSend.Key.ComponentId == SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID)
		{
			CrossServer::SenderState& SenderState = ActorSenderState.FindChecked(UpdateToSend.Key.EntityId);
			RPCRingBufferDescriptor Descriptor = RPCRingBufferUtils::GetRingBufferDescriptor(ERPCType::CrossServerSender);

			SenderState.Alloc.ForeachClearedSlot([&](uint32 ToClear) {
				uint32 Field = Descriptor.GetRingBufferElementFieldId(ERPCType::CrossServerSender, ToClear + 1);

				Schema_AddComponentUpdateClearedField(UpdateToSend.Value.Update, Field);
				Schema_AddComponentUpdateClearedField(UpdateToSend.Value.Update, Field + 1);
			});
		}

		if (UpdateToSend.Key.ComponentId == SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID)
		{
			CrossServer::SlotAlloc& SlotAlloc = ACKAllocMap.FindChecked(UpdateToSend.Key.EntityId);

			SlotAlloc.ForeachClearedSlot([&](uint32 ToClear) {
				Schema_AddComponentUpdateClearedField(UpdateToSend.Value.Update, 2 + ToClear);
			});
		}
	}
}

} // namespace SpatialGDK

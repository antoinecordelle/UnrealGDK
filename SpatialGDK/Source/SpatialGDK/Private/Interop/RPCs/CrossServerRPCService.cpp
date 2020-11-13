// Copyright (c) Improbable Worlds Ltd, All Rights Reserved

#include "Interop/RPCs/CrossServerRPCService.h"

#include "EngineClasses/SpatialPackageMapClient.h"
#include "SpatialConstants.h"
#include "Utils/RepLayoutUtils.h"

DEFINE_LOG_CATEGORY(LogCrossServerRPCService);

#pragma optimize("", off)

namespace SpatialGDK
{
CrossServerRPCService::CrossServerRPCService(const ExtractRPCDelegate InExtractRPCCallback, const FSubView& InSubView,
											 /*USpatialNetDriver* InNetDriver,*/ FRPCStore& InRPCStore)
	: ExtractRPCCallback(InExtractRPCCallback)
	, SubView(&InSubView)
	//, NetDriver(InNetDriver)
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
		Endpoints = &CrossServerDataStore.Add(Sender.Entity);
	}
	else
	{
		EndpointObject = Schema_GetComponentUpdateFields(RPCStore->GetOrCreateComponentUpdate(SenderEndpointId, nullptr));
	}

	CrossServer::WriterState& SenderState = Endpoints->SenderState;

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

void CrossServerRPCService::AdvanceView()
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
			break;
		case EntityDelta::REMOVE:
			CrossServerDataStore.Remove(Delta.EntityId);
			RPCStore->PendingComponentUpdatesToSend.Remove(
				EntityComponentId(Delta.EntityId, SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID));
			RPCStore->PendingComponentUpdatesToSend.Remove(
				EntityComponentId(Delta.EntityId, SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID));
			break;
		case EntityDelta::TEMPORARILY_REMOVED:
			CrossServerDataStore.Remove(Delta.EntityId);
			PopulateDataStore(Delta.EntityId);
			break;
		default:
			break;
		}
	}
}

void CrossServerRPCService::ProcessChanges()
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
				ProcessComponentChange(Delta.EntityId, Change.ComponentId);
			}
			break;
		}
		case EntityDelta::ADD:
			EntityAdded(Delta.EntityId);
			break;
		case EntityDelta::REMOVE:

			break;
		case EntityDelta::TEMPORARILY_REMOVED:
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
	CrossServerEndpoints* Endpoints = CrossServerDataStore.Find(EntityId);
	HandleRPC(EntityId, *Endpoints->ReceivedRPCs);
	UpdateSentRPCsACKs(EntityId, *Endpoints->ACKedRPCs);
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
			Endpoints->ReceivedRPCs->ApplyComponentUpdate(Update);
			break;

		case SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID:
			Endpoints->ACKedRPCs->ApplyComponentUpdate(Update);
			break;
		default:
			break;
		}
	}
}

void CrossServerRPCService::ProcessComponentChange(const Worker_EntityId EntityId, const Worker_ComponentId ComponentId)
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
			HandleRPC(EntityId, Endpoints->ReceivedRPCs.GetValue());
			break;

		case SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID:
			UpdateSentRPCsACKs(EntityId, Endpoints->ACKedRPCs.GetValue());
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

	CrossServerEndpoints& NewEntry = CrossServerDataStore.FindOrAdd(EntityId);
	NewEntry.ACKedRPCs.Emplace(CrossServerEndpointSenderACK(SenderACKData));
	NewEntry.ReceivedRPCs.Emplace(CrossServerEndpointReceiver(ReceiverData));
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
		CrossServer::WriterState& SenderState = CrossServerDataStore.FindChecked(EntityId).SenderState;
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
				NewEntry.Target = RPCTarget(TargetRef.GetValue());
				NewEntry.SourceSlot = SlotIdx;

				SenderState.Mailbox.Add(RPCKey, NewEntry);
				SenderState.Alloc.Occupied[SlotIdx] = true;
			}
		}
		break;
	}
	case SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID:
	{
		CrossServerEndpointReceiverACK ReceiverACKEndpoint(Data->GetUnderlying());
		CrossServer::ReaderState& ReceiverACKState = CrossServerDataStore.FindChecked(EntityId).ReceiverACKState;

		uint32 numAcks = 0;
		for (int32 SlotIdx = 0; SlotIdx < ReceiverACKEndpoint.ACKArray.Num(); ++SlotIdx)
		{
			const TOptional<ACKItem>& ACK = ReceiverACKEndpoint.ACKArray[SlotIdx];
			if (ACK)
			{
				CrossServer::RPCSlots NewSlot;
				NewSlot.CounterpartEntity = ACK->Sender;
				NewSlot.ACKSlot = SlotIdx;

				ReceiverACKState.RPCSlots.Add(CrossServer::RPCKey(ACK->Sender, ACK->RPCId), NewSlot);
				ReceiverACKState.ACKAlloc.CommitSlot(SlotIdx);
			}
		}
		break;
	}
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

	// Temporary removal on the dependency on the net driver.
	//
	// if (SubView->HasAuthority(EntityId, SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID))
	//{
	//	const TWeakObjectPtr<UObject> ActorReceivingRPC = NetDriver->PackageMap->GetObjectFromEntityId(EntityId);
	//	if (!ActorReceivingRPC.IsValid())
	//	{
	//		UE_LOG(LogCrossServerRPCService, Log,
	//			   TEXT("Entity receiving ring buffer RPC does not exist in PackageMap, possibly due to corresponding actor getting "
	//					"destroyed. Entity: %lld, Component: %d"),
	//			   EntityId, SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID);
	//		return;
	//	}
	//
	//	const bool bActorRoleIsSimulatedProxy = Cast<AActor>(ActorReceivingRPC.Get())->Role == ROLE_SimulatedProxy;
	//	if (bActorRoleIsSimulatedProxy)
	//	{
	//		UE_LOG(LogCrossServerRPCService, Verbose,
	//			   TEXT("Will not process server RPC, Actor role changed to SimulatedProxy. This happens on migration. Entity: %lld"),
	//			   EntityId);
	//		return;
	//	}
	//}
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

	CrossServerEndpoints& Endpoint = CrossServerDataStore.FindChecked(EndpointId);

	for (uint32 SlotIdx = 0; SlotIdx < RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerReceiver); ++SlotIdx)
	{
		const TOptional<RPCPayload>& Element = Buffer.RingBuffer[SlotIdx];
		if (Element.IsSet())
		{
			const TOptional<CrossServerRPCInfo>& Counterpart = Buffer.Counterpart[SlotIdx];
			if (ensure(Counterpart.IsSet()))
			{
				CrossServer::RPCKey RPCKey(Counterpart->Entity, Counterpart->RPCId);

				const bool bAlreadyQueued = Endpoint.ReceiverACKState.RPCSlots.Find(RPCKey) != nullptr;

				if (!bAlreadyQueued)
				{
					CrossServer::RPCSlots& NewSlots = Endpoint.ReceiverACKState.RPCSlots.Add(RPCKey);
					NewSlots.CounterpartSlot = SlotIdx;
					Endpoint.ReceiverSchedule.Add(RPCKey);
				}
			}
		}
	}

	while (!Endpoint.ReceiverSchedule.IsEmpty())
	{
		CrossServer::RPCKey RPC = Endpoint.ReceiverSchedule.Peek();
		CrossServer::RPCSlots& Slots = Endpoint.ReceiverACKState.RPCSlots.FindChecked(RPC);

		Endpoint.ReceiverSchedule.Extract();

		const RPCPayload& Payload = Buffer.RingBuffer[Slots.CounterpartSlot].GetValue();

		// const bool bKeepExtracting =
		ExtractRPCCallback.Execute(FUnrealObjectRef(EndpointId, Payload.Offset), RPCSender(CrossServerRPCInfo(RPC.Get<0>(), RPC.Get<1>())),
								   Payload);
		// if (!bKeepExtracting)
		//{
		//	break;
		//}
	}
}

void CrossServerRPCService::WriteCrossServerACKFor(Worker_EntityId Receiver, const RPCSender& Sender)
{
	CrossServerEndpoints& Endpoint = CrossServerDataStore.FindChecked(Receiver);
	TOptional<uint32> ReservedSlot = Endpoint.ReceiverACKState.ACKAlloc.ReserveSlot();
	check(ReservedSlot.IsSet());
	uint32 SlotIdx = ReservedSlot.GetValue();

	ACKItem ACK;
	ACK.RPCId = Sender.RPCId;
	ACK.Sender = Sender.Entity;

	EntityComponentId Pair(Receiver, SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID);

	Schema_ComponentUpdate* Update = RPCStore->GetOrCreateComponentUpdate(Pair, nullptr);
	Schema_Object* UpdateObject = Schema_GetComponentUpdateFields(Update);
	// Schema_AddUint64(UpdateObject, 1, ACKComponent->RPCAck);
	Schema_Object* NewEntry = Schema_AddObject(UpdateObject, 2 + SlotIdx);
	ACK.WriteToSchema(NewEntry);

	CrossServer::RPCSlots& OccupiedSlot = Endpoint.ReceiverACKState.RPCSlots.FindChecked(CrossServer::RPCKey(Sender.Entity, Sender.RPCId));
	OccupiedSlot.ACKSlot = SlotIdx;
}

void CrossServerRPCService::UpdateSentRPCsACKs(Worker_EntityId SenderId, const CrossServerEndpointSenderACK& ACKComponent)
{
	for (int32 SlotIdx = 0; SlotIdx < ACKComponent.ACKArray.Num(); ++SlotIdx)
	{
		if (ACKComponent.ACKArray[SlotIdx])
		{
			const ACKItem& ACK = ACKComponent.ACKArray[SlotIdx].GetValue();

			CrossServer::RPCKey RPCKey(ACK.Sender, ACK.RPCId);

			CrossServer::WriterState& SenderState = CrossServerDataStore.FindChecked(SenderId).SenderState;
			CrossServer::SentRPCEntry* SentRPC = SenderState.Mailbox.Find(RPCKey);
			if (SentRPC != nullptr)
			{
				SenderState.Alloc.FreeSlot(SentRPC->SourceSlot);
				SenderState.Mailbox.Remove(RPCKey);

				EntityComponentId Pair(ACK.Sender, SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID);
				RPCStore->GetOrCreateComponentUpdate(Pair, nullptr);
			}
		}
	}
}

void CrossServerRPCService::CleanupACKsFor(Worker_EntityId EndpointId, const CrossServerEndpointReceiver& Receiver)
{
	CrossServerEndpoints& Endpoint = CrossServerDataStore.FindChecked(EndpointId);
	CrossServer::ReaderState& State = Endpoint.ReceiverACKState;

	if (State.RPCSlots.Num() > 0)
	{
		CrossServer::ReadRPCMap ACKSToClear = State.RPCSlots;
		for (auto Iterator = ACKSToClear.CreateIterator(); Iterator; ++Iterator)
		{
			if (Iterator->Value.ACKSlot == -1)
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

		EntityComponentId Pair(EndpointId, SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID);

		for (auto const& SlotToClear : ACKSToClear)
		{
			uint32 SlotIdx = SlotToClear.Value.ACKSlot;
			State.RPCSlots.Remove(SlotToClear.Key);

			RPCStore->GetOrCreateComponentUpdate(Pair, nullptr);

			State.ACKAlloc.FreeSlot(SlotIdx);
		}
	}
}

void CrossServerRPCService::FlushPendingClearedFields()
{
	for (auto& UpdateToSend : RPCStore->PendingComponentUpdatesToSend)
	{
		if (UpdateToSend.Key.ComponentId == SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID)
		{
			CrossServer::WriterState& SenderState = CrossServerDataStore.FindChecked(UpdateToSend.Key.EntityId).SenderState;
			RPCRingBufferDescriptor Descriptor = RPCRingBufferUtils::GetRingBufferDescriptor(ERPCType::CrossServerSender);

			SenderState.Alloc.ForeachClearedSlot([&](uint32 ToClear) {
				uint32 Field = Descriptor.GetRingBufferElementFieldId(ERPCType::CrossServerSender, ToClear + 1);

				Schema_AddComponentUpdateClearedField(UpdateToSend.Value.Update, Field);
				Schema_AddComponentUpdateClearedField(UpdateToSend.Value.Update, Field + 1);
			});
		}

		if (UpdateToSend.Key.ComponentId == SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID)
		{
			CrossServer::SlotAlloc& SlotAlloc = CrossServerDataStore.FindChecked(UpdateToSend.Key.EntityId).ReceiverACKState.ACKAlloc;

			SlotAlloc.ForeachClearedSlot([&](uint32 ToClear) {
				Schema_AddComponentUpdateClearedField(UpdateToSend.Value.Update, 2 + ToClear);
			});
		}
	}
}

} // namespace SpatialGDK

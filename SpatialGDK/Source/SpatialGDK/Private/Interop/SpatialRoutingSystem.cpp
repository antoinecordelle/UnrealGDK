#include "Interop/SpatialRoutingSystem.h"
#include "Interop/Connection/SpatialOSWorkerInterface.h"
#include "Schema/ServerWorker.h"
#include "Utils/InterestFactory.h"

DEFINE_LOG_CATEGORY(LogSpatialRoutingSystem);

#pragma optimize("", off)

namespace SpatialGDK
{
void SpatialRoutingSystem::ProcessUpdate(Worker_EntityId Entity, const ComponentChange& Change, RoutingComponents& Components)
{
	switch (Change.ComponentId)
	{
	case SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID:
		switch (Change.Type)
		{
		case ComponentChange::COMPLETE_UPDATE:
		{
			Components.Sender.Emplace(CrossServerEndpointSender(Change.CompleteUpdate.Data));

			break;
		}
		case ComponentChange::UPDATE:
		{
			Components.Sender->ApplyComponentUpdate(Change.Update);
			break;
		}
		}
		OnSenderChanged(Entity, Components);
		break;
	case SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID:
	case SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID:
		check(Change.Type == ComponentChange::COMPLETE_UPDATE);
		break;
	case SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID:
		switch (Change.Type)
		{
		case ComponentChange::COMPLETE_UPDATE:
		{
			Components.ReceiverACK.Emplace(CrossServerEndpointReceiverACK(Change.CompleteUpdate.Data));
			break;
		}

		case ComponentChange::UPDATE:
		{
			Components.ReceiverACK->ApplyComponentUpdate(Change.Update);
			break;
		}
		}
		OnReceiverACKChanged(Entity, Components);
		break;
	default:
		checkNoEntry();
		break;
	}
}

void SpatialRoutingSystem::OnSenderChanged(Worker_EntityId SenderId, RoutingComponents& Components)
{
	TMap<CrossServer::RPCKey, Worker_EntityId> ReceiverDisappeared;
	CrossServer::ReadRPCMap SlotsToClear = Components.SenderACKState.RPCSlots;

	const RPCRingBuffer& Buffer = Components.Sender->ReliableRPCBuffer;

	// First loop to free Receiver slots.
	for (uint32 SlotIdx = 0; SlotIdx < RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerSender); ++SlotIdx)
	{
		const TOptional<RPCPayload>& Element = Buffer.RingBuffer[SlotIdx];
		if (Element.IsSet())
		{
			const TOptional<CrossServerRPCInfo>& Counterpart = Buffer.Counterpart[SlotIdx];

			Worker_EntityId Receiver = Counterpart->Entity;
			uint64 RPCId = Counterpart->RPCId;
			CrossServer::RPCKey RPCKey(SenderId, RPCId);
			SlotsToClear.Remove(RPCKey);

			if (RoutingComponents* ReceiverComps = RoutingWorkerView.Find(Receiver))
			{
				if (ReceiverComps->ReceiverState.Mailbox.Find(RPCKey) == nullptr)
				{
					CrossServer::SentRPCEntry Entry;
					// Entry.RPCId = RPCKey;
					Entry.Target = RPCTarget(*Counterpart);
					// Entry.Timestamp = FPlatformTime::Cycles64();
					Entry.SourceSlot = SlotIdx;

					ReceiverComps->ReceiverState.Mailbox.Add(RPCKey, Entry);
					ReceiverComps->ReceiverSchedule.Add(RPCKey);
					ReceiversToInspect.Add(Receiver);
				}
			}
			else
			{
				ReceiverDisappeared.Add(RPCKey, Receiver);
			}
		}
	}

	for (auto const& SlotToClear : SlotsToClear)
	{
		const CrossServer::RPCSlots& Slots = SlotToClear.Value;
		{
			EntityComponentId SenderPair(SenderId, SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID);
			Components.SenderACKState.ACKAlloc.FreeSlot(Slots.ACKSlot);

			GetOrCreateComponentUpdate(SenderPair);
		}

		if (RoutingComponents* ReceiverComps = RoutingWorkerView.Find(Slots.CounterpartEntity))
		{
			ClearReceiverSlot(Slots.CounterpartEntity, SlotToClear.Key, *ReceiverComps);
		}

		Components.SenderACKState.RPCSlots.Remove(SlotToClear.Key);
	}

	for (auto RPC : ReceiverDisappeared)
	{
		auto& Slots = Components.SenderACKState.RPCSlots.FindOrAdd(RPC.Key);
		if (Slots.ACKSlot < 0)
		{
			Slots.CounterpartEntity = RPC.Value;
			WriteACKToSender(RPC.Key, Components);
		}
	}
}

void SpatialRoutingSystem::ClearReceiverSlot(Worker_EntityId Receiver, CrossServer::RPCKey RPCKey, RoutingComponents& ReceiverComponents)
{
	CrossServer::SentRPCEntry* SentRPC = ReceiverComponents.ReceiverState.Mailbox.Find(RPCKey);
	check(SentRPC != nullptr);

	EntityComponentId ReceiverPair(Receiver, SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID);

	check(SentRPC->DestinationSlot.IsSet());
	uint32 SlotIdx = SentRPC->DestinationSlot.GetValue();

	ReceiverComponents.ReceiverState.Mailbox.Remove(RPCKey);
	ReceiverComponents.ReceiverState.Alloc.FreeSlot(SlotIdx);

	GetOrCreateComponentUpdate(ReceiverPair);
	if (!ReceiverComponents.ReceiverSchedule.IsEmpty())
	{
		ReceiversToInspect.Add(ReceiverPair.Get<0>());
	}
}

void SpatialRoutingSystem::TransferRPCsToReceiver(Worker_EntityId ReceiverId, RoutingComponents& Components)
{
	if (RoutingComponents* ReceiverComps = RoutingWorkerView.Find(ReceiverId))
	{
		while (!ReceiverComps->ReceiverSchedule.IsEmpty())
		{
			TOptional<uint32> FreeSlot = ReceiverComps->ReceiverState.Alloc.PeekFreeSlot();
			if (!FreeSlot)
			{
				return;
			}
			CrossServer::RPCKey RPCToSend = ReceiverComps->ReceiverSchedule.Extract();
			CrossServer::SentRPCEntry& SentRPC = ReceiverComps->ReceiverState.Mailbox.FindChecked(RPCToSend);

			check(!SentRPC.DestinationSlot.IsSet());

			Worker_EntityId SenderId = RPCToSend.Get<0>();
			uint64 RPCId = RPCToSend.Get<1>();

			RoutingComponents* SenderComps = RoutingWorkerView.Find(SenderId);

			if (!SenderComps)
			{
				ReceiverComps->ReceiverState.Mailbox.Remove(RPCToSend);
				// Sender disappeared before we could deliver the RPC :/ copy payload ? tombstone? drop?
				continue;
			}

			const TOptional<RPCPayload>& Element = SenderComps->Sender->ReliableRPCBuffer.RingBuffer[SentRPC.SourceSlot];
			check(Element.IsSet());

			Schema_ComponentUpdate* ReceiverUpdate =
				GetOrCreateComponentUpdate(EntityComponentId(ReceiverId, SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID));
			Schema_Object* EndpointObject = Schema_GetComponentUpdateFields(ReceiverUpdate);

			const uint32 SlotIdx = FreeSlot.GetValue();
			CrossServer::WritePayloadAndCounterpart(EndpointObject, *Element, CrossServerRPCInfo(SenderId, RPCId), SlotIdx);

			SentRPC.DestinationSlot = SlotIdx;
			ReceiverComps->ReceiverState.Alloc.CommitSlot(SlotIdx);

			CrossServer::RPCSlots NewSlot;
			NewSlot.CounterpartEntity = ReceiverId;
			NewSlot.CounterpartSlot = SlotIdx;

			SenderComps->SenderACKState.RPCSlots.Add(RPCToSend) = NewSlot;
		}
	}
}

void SpatialRoutingSystem::WriteACKToSender(CrossServer::RPCKey RPCKey, RoutingComponents& SenderComponents)
{
	// Both SentRPC and Slots entries should be cleared at the same time.
	CrossServer::RPCSlots* Slots = SenderComponents.SenderACKState.RPCSlots.Find(RPCKey);
	check(Slots != nullptr);

	if (Slots->ACKSlot < 0)
	{
		if (TOptional<uint32_t> ReservedSlot = SenderComponents.SenderACKState.ACKAlloc.ReserveSlot())
		{
			Slots->ACKSlot = ReservedSlot.GetValue();
			EntityComponentId SenderPair(RPCKey.Get<0>(), SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID);
			Schema_ComponentUpdate* Update = GetOrCreateComponentUpdate(SenderPair);
			Schema_Object* UpdateObject = Schema_GetComponentUpdateFields(Update);

			ACKItem SenderACK;
			SenderACK.Sender = RPCKey.Get<0>();
			SenderACK.RPCId = RPCKey.Get<1>();

			Schema_Object* NewEntry = Schema_AddObject(UpdateObject, 2 + Slots->ACKSlot);
			SenderACK.WriteToSchema(NewEntry);
		}
		else
		{
			checkNoEntry();
			// Out of free slots, should not be possible.
			UE_LOG(LogTemp, Log, TEXT("Out of sender ACK slot"));
		}
	}
	else
	{
		// Already taken care of.
	}
}

void SpatialRoutingSystem::OnReceiverACKChanged(Worker_EntityId EntityId, RoutingComponents& Components)
{
	CrossServerEndpointReceiverACK& ReceiverACK = Components.ReceiverACK.GetValue();
	for (int32 SlotIdx = 0; SlotIdx < ReceiverACK.ACKArray.Num(); ++SlotIdx)
	{
		if (ReceiverACK.ACKArray[SlotIdx])
		{
			const ACKItem& ReceiverACKItem = ReceiverACK.ACKArray[SlotIdx].GetValue();

			CrossServer::RPCKey RPCKey(ReceiverACKItem.Sender, ReceiverACKItem.RPCId);
			CrossServer::SentRPCEntry* SentRPC = Components.ReceiverState.Mailbox.Find(RPCKey);
			if (SentRPC == nullptr)
			{
				continue;
			}

			RoutingComponents* SenderComponents = RoutingWorkerView.Find(ReceiverACKItem.Sender);
			if (SenderComponents != nullptr)
			{
				WriteACKToSender(RPCKey, *SenderComponents);
			}
			else
			{
				// Sender disappeared, clear Receiver slot.
				ClearReceiverSlot(EntityId, RPCKey, Components);
			}
		}
	}
}

Schema_ComponentUpdate* SpatialRoutingSystem::GetOrCreateComponentUpdate(
	TPair<Worker_EntityId_Key, Worker_ComponentId> EntityComponentIdPair)
{
	check(EntityComponentIdPair.Key != 0);
	Schema_ComponentUpdate** ComponentUpdatePtr = PendingComponentUpdatesToSend.Find(EntityComponentIdPair);
	if (ComponentUpdatePtr == nullptr)
	{
		ComponentUpdatePtr = &PendingComponentUpdatesToSend.Add(EntityComponentIdPair, Schema_CreateComponentUpdate());
	}
	return *ComponentUpdatePtr;
}

void SpatialRoutingSystem::Advance(SpatialOSWorkerInterface* Connection)
{
	const TArray<Worker_Op>& Messages = Connection->GetWorkerMessages();
	for (const auto& Message : Messages)
	{
		switch (Message.op_type)
		{
		case WORKER_OP_TYPE_RESERVE_ENTITY_IDS_RESPONSE:
		{
			const Worker_ReserveEntityIdsResponseOp& Op = Message.op.reserve_entity_ids_response;
			if (Op.request_id == RoutingWorkerEntityRequest)
			{
				if (Op.first_entity_id == SpatialConstants::INVALID_ENTITY_ID)
				{
					UE_LOG(LogSpatialRoutingSystem, Error, TEXT("Reserve entity failed : %s"), UTF8_TO_TCHAR(Op.message));
					RoutingWorkerEntityRequest = 0;
				}
				else
				{
					RoutingWorkerEntity = Message.op.reserve_entity_ids_response.first_entity_id;
					CreateRoutingWorkerEntity(Connection);
				}
			}
			break;
		}
		case WORKER_OP_TYPE_CREATE_ENTITY_RESPONSE:
		{
			const Worker_CreateEntityResponseOp& Op = Message.op.create_entity_response;
			if (Op.request_id == RoutingWorkerEntityRequest)
			{
				if (Op.entity_id == SpatialConstants::INVALID_ENTITY_ID)
				{
					UE_LOG(LogSpatialRoutingSystem, Error, TEXT("Create entity failed : %s"), UTF8_TO_TCHAR(Op.message));
					RoutingWorkerEntityRequest = 0;
				}
			}
		}
		break;
		}
	}

	const FSubViewDelta& SubViewDelta = SubView.GetViewDelta();
	for (const EntityDelta& Delta : SubViewDelta.EntityDeltas)
	{
		switch (Delta.Type)
		{
		case EntityDelta::UPDATE:
		{
			RoutingComponents& Components = RoutingWorkerView.FindChecked(Delta.EntityId);
			for (const ComponentChange& Change : Delta.ComponentUpdates)
			{
				ProcessUpdate(Delta.EntityId, Change, Components);
			}

			for (const ComponentChange& Change : Delta.ComponentsRefreshed)
			{
				ProcessUpdate(Delta.EntityId, Change, Components);
			}
		}
		break;
		case EntityDelta::ADD:
		{
			const EntityViewElement& EntityView = SubView.GetView().FindChecked(Delta.EntityId);
			RoutingComponents& Components = RoutingWorkerView.Add(Delta.EntityId);

			for (const auto& ComponentDesc : EntityView.Components)
			{
				switch (ComponentDesc.GetComponentId())
				{
				case SpatialConstants::CROSSSERVER_SENDER_ENDPOINT_COMPONENT_ID:
					Components.Sender = CrossServerEndpointSender(ComponentDesc.GetUnderlying());
					// Should inspect the component if we were reloading a snapshot
					break;
				case SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID:
				{
					CrossServerEndpointSenderACK TempView(ComponentDesc.GetUnderlying());
					for (int32 SlotIdx = 0; SlotIdx < TempView.ACKArray.Num(); ++SlotIdx)
					{
						const TOptional<ACKItem>& Slot = TempView.ACKArray[SlotIdx];
						if (Slot.IsSet())
						{
							CrossServer::RPCSlots& Slots =
								Components.SenderACKState.RPCSlots.FindOrAdd(CrossServer::RPCKey(Slot->Sender, Slot->RPCId));
							Slots.ACKSlot = SlotIdx;
							Components.SenderACKState.ACKAlloc.CommitSlot(SlotIdx);
						}
					}
				}
				break;
				case SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID:
				{
					CrossServerEndpointReceiver TempView(ComponentDesc.GetUnderlying());
					for (int32 SlotIdx = 0; SlotIdx < TempView.ReliableRPCBuffer.RingBuffer.Num(); ++SlotIdx)
					{
						const auto& Slot = TempView.ReliableRPCBuffer.RingBuffer[SlotIdx];
						if (Slot.IsSet())
						{
							const TOptional<CrossServerRPCInfo>& SenderBackRef = TempView.ReliableRPCBuffer.Counterpart[SlotIdx];
							check(SenderBackRef.IsSet());

							CrossServer::RPCKey RPCKey(SenderBackRef->Entity, SenderBackRef->RPCId);
							CrossServer::SentRPCEntry NewEntry;
							// NewEntry.RPCId = RPCKey;
							// NewEntry.SourceSlot = ??
							NewEntry.DestinationSlot = SlotIdx;
							NewEntry.Target = RPCTarget(*SenderBackRef);
							// NewEntry.Timestamp = 0;
							// NewEntry.EntityRequest = 0;

							Components.ReceiverState.Mailbox.Add(RPCKey, NewEntry);
							Components.ReceiverState.Alloc.CommitSlot(SlotIdx);

							RoutingComponents& SenderComponents = RoutingWorkerView.FindOrAdd(NewEntry.Target.Entity);
							// If we were reloading a snapshot, have to check that the sender still exists.
							CrossServer::RPCSlots& Slots = SenderComponents.SenderACKState.RPCSlots.FindOrAdd(RPCKey);
							Slots.CounterpartEntity = Delta.EntityId;
							Slots.CounterpartSlot = SlotIdx;
						}
					}
				}
				break;
				case SpatialConstants::CROSSSERVER_RECEIVER_ACK_ENDPOINT_COMPONENT_ID:
					Components.ReceiverACK = CrossServerEndpointReceiverACK(ComponentDesc.GetUnderlying());
					// Should inspect the component if we were reloading a snapshot
					break;
				}
			}
		}
		break;
		case EntityDelta::REMOVE:
		case EntityDelta::TEMPORARILY_REMOVED:
		{
			ReceiversToInspect.Remove(Delta.EntityId);
			PendingComponentUpdatesToSend.Remove(
				EntityComponentId(Delta.EntityId, SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID));
			PendingComponentUpdatesToSend.Remove(
				EntityComponentId(Delta.EntityId, SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID));

			if (RoutingComponents* Components = RoutingWorkerView.Find(Delta.EntityId))
			{
				for (auto MailboxItem : Components->ReceiverState.Mailbox)
				{
					CrossServer::SentRPCEntry& Entry = MailboxItem.Value;
					if (Entry.DestinationSlot)
					{
						RoutingComponents* SenderComponents = RoutingWorkerView.Find(MailboxItem.Key.Get<0>());
						if (SenderComponents != nullptr)
						{
							WriteACKToSender(MailboxItem.Key, *SenderComponents);
						}
					}
				}

				for (auto Slots : Components->SenderACKState.RPCSlots)
				{
					Worker_EntityId Receiver = Slots.Value.CounterpartEntity;
					if (Receiver != SpatialConstants::INVALID_ENTITY_ID && Slots.Value.ACKSlot != -1)
					{
						// The receiver would be waiting for an update from the sender.
						RoutingComponents* ReceiverComponents = RoutingWorkerView.Find(Receiver);
						if (ReceiverComponents)
						{
							ClearReceiverSlot(Receiver, Slots.Key, *ReceiverComponents);
						}
					}
				}

				RoutingWorkerView.Remove(Delta.EntityId);
			}
		}
		break;
		default:
			break;
		}
	}

	for (auto Receiver : ReceiversToInspect)
	{
		TransferRPCsToReceiver(Receiver, RoutingWorkerView.FindChecked(Receiver));
	}
	ReceiversToInspect.Empty();
}

void SpatialRoutingSystem::Flush(SpatialOSWorkerInterface* Connection)
{
	for (auto& Entry : PendingComponentUpdatesToSend)
	{
		Worker_EntityId Entity = Entry.Key.Get<0>();
		Worker_ComponentId CompId = Entry.Key.Get<1>();

		if (RoutingComponents* Components = RoutingWorkerView.Find(Entity))
		{
			if (CompId == SpatialConstants::CROSSSERVER_RECEIVER_ENDPOINT_COMPONENT_ID)
			{
				RPCRingBufferDescriptor Descriptor = RPCRingBufferUtils::GetRingBufferDescriptor(ERPCType::CrossServerReceiver);

				Components->ReceiverState.Alloc.ForeachClearedSlot([&](uint32_t ToClear) {
					uint32 Field = Descriptor.GetRingBufferElementFieldId(ERPCType::CrossServerReceiver, ToClear + 1);

					Schema_AddComponentUpdateClearedField(Entry.Value, Field);
					Schema_AddComponentUpdateClearedField(Entry.Value, Field + 1);
				});
			}

			if (CompId == SpatialConstants::CROSSSERVER_SENDER_ACK_ENDPOINT_COMPONENT_ID)
			{
				Components->SenderACKState.ACKAlloc.ForeachClearedSlot([&](uint32_t ToClear) {
					Schema_AddComponentUpdateClearedField(Entry.Value, ToClear + 2);
				});
			}
		}
		FWorkerComponentUpdate Update;
		Update.component_id = CompId;
		Update.schema_type = Entry.Value;
		Connection->SendComponentUpdate(Entity, &Update);
	}
	PendingComponentUpdatesToSend.Empty();
}

void SpatialRoutingSystem::Init(SpatialOSWorkerInterface* Connection)
{
	RoutingWorkerEntityRequest = Connection->SendReserveEntityIdsRequest(1);
}

void SpatialRoutingSystem::CreateRoutingWorkerEntity(SpatialOSWorkerInterface* Connection)
{
	const WorkerRequirementSet WorkerIdPermission{ { FString::Printf(TEXT("workerId:%s"), *RoutingWorkerId) } };

	WriteAclMap ComponentWriteAcl;
	ComponentWriteAcl.Add(SpatialConstants::POSITION_COMPONENT_ID, WorkerIdPermission);
	ComponentWriteAcl.Add(SpatialConstants::METADATA_COMPONENT_ID, WorkerIdPermission);
	ComponentWriteAcl.Add(SpatialConstants::ENTITY_ACL_COMPONENT_ID, WorkerIdPermission);
	ComponentWriteAcl.Add(SpatialConstants::INTEREST_COMPONENT_ID, WorkerIdPermission);
	// ComponentWriteAcl.Add(SpatialConstants::SERVER_WORKER_COMPONENT_ID, WorkerIdPermission);

	TArray<FWorkerComponentData> Components;
	Components.Add(Position().CreatePositionData());
	Components.Add(Metadata(FString::Printf(TEXT("WorkerEntity:%s"), *RoutingWorkerId)).CreateMetadataData());
	Components.Add(EntityAcl(WorkerIdPermission, ComponentWriteAcl).CreateEntityAclData());
	// Components.Add(ServerWorker(Connection->GetWorkerId(), false).CreateServerWorkerData());

	Components.Add(InterestFactory::CreateRoutingWorkerInterest().CreateInterestData());

	RoutingWorkerEntityRequest = Connection->SendCreateEntityRequest(Components, &RoutingWorkerEntity);
}

void SpatialRoutingSystem::Destroy(SpatialOSWorkerInterface* Connection)
{
	Connection->SendDeleteEntityRequest(RoutingWorkerEntity);
}

} // namespace SpatialGDK

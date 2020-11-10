

#pragma once

#include "SpatialCommonTypes.h"
#include "Utils/RPCRingBuffer.h"

#include "Containers/BitArray.h"

namespace SpatialGDK
{
namespace CrossServer
{
typedef TPair<Worker_EntityId_Key, uint64> RPCKey;

struct SentRPCEntry
{
	bool operator==(SentRPCEntry const& iRHS) const { return FMemory::Memcmp(this, &iRHS, sizeof(SentRPCEntry)) == 0; }

	// RPCKey RPCId;
	// Worker_EntityId Target;
	RPCTarget Target;
	// uint64 Timestamp;
	uint32 SourceSlot;
	TOptional<uint32> DestinationSlot;
	// TOptional<Worker_RequestId> EntityRequest;
};

struct SlotAlloc
{
	SlotAlloc()
	{
		Occupied.Init(false, RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerSender));
		ToClear.Init(false, RPCRingBufferUtils::GetRingBufferSize(ERPCType::CrossServerSender));
	}

	TOptional<uint32_t> PeekFreeSlot()
	{
		int32 freeSlot = Occupied.Find(false);
		if (freeSlot >= 0)
		{
			return freeSlot;
		}

		return {};
	}

	void CommitSlot(uint32_t Slot)
	{
		Occupied[Slot] = true;
		ToClear[Slot] = false;
	}

	TOptional<uint32_t> ReserveSlot()
	{
		int32 freeSlot = Occupied.FindAndSetFirstZeroBit();
		if (freeSlot >= 0)
		{
			CommitSlot(freeSlot);
			return freeSlot;
		}

		return {};
	}

	void FreeSlot(uint32_t Slot)
	{
		Occupied[Slot] = false;
		ToClear[Slot] = true;
	}

	template <typename Functor>
	void ForeachClearedSlot(Functor&& Fun)
	{
		for (int32 ToClearIdx = ToClear.Find(true); ToClearIdx >= 0; ToClearIdx = ToClear.Find(true))
		{
			Fun(ToClearIdx);
			ToClear[ToClearIdx] = false;
		}
	}

	TBitArray<FDefaultBitArrayAllocator> Occupied;
	TBitArray<FDefaultBitArrayAllocator> ToClear;
};

struct RPCSchedule
{
	void Add(RPCKey RPC)
	{
		int32 ScheduleSize = SendingSchedule.Num();
		SendingSchedule.Add(RPC);
		RPCKey& LastAddition = SendingSchedule[ScheduleSize];
		for (int32 i = 0; i < ScheduleSize; ++i)
		{
			RPCKey& Item = SendingSchedule[i];
			if (Item.Get<0>() == LastAddition.Get<0>())
			{
				// Enforce ordering on same senders while keeping different senders order of arrival to avoid starvation.
				if (Item.Get<1>() > LastAddition.Get<1>())
				{
					Swap(Item, LastAddition);
				}
			}
		}
	}

	RPCKey Peek() { return SendingSchedule[0]; }

	RPCKey Extract()
	{
		RPCKey NextRPC = SendingSchedule[0];
		SendingSchedule.RemoveAt(0);
		return NextRPC;
	}

	bool IsEmpty() const { return SendingSchedule.Num() == 0; }

	TArray<RPCKey> SendingSchedule;
};

struct SenderState
{
	uint64 LastSentRPCId = 0;
	TMap<RPCKey, SentRPCEntry> Mailbox;
	RPCSchedule Schedule;
	SlotAlloc Alloc;
};

struct ACKSlot
{
	bool operator==(const ACKSlot& Other) const { return Receiver == Other.Receiver && Slot == Other.Slot; }

	Worker_EntityId Receiver = 0;
	uint32 Slot = -1;
};

struct RPCSlots
{
	ACKSlot ReceiverSlot;
	int32 SenderACKSlot = -1;
};

typedef TMap<RPCKey, RPCSlots> RPCAllocMap;

inline void WritePayloadAndCounterpart(Schema_Object* EndpointObject, const RPCPayload& Payload, const CrossServerRPCInfo& Info,
									   uint32_t SlotIdx)
{
	RPCRingBufferDescriptor Descriptor = RPCRingBufferUtils::GetRingBufferDescriptor(ERPCType::CrossServerReceiver);
	uint32 Field = Descriptor.GetRingBufferElementFieldId(ERPCType::CrossServerReceiver, SlotIdx + 1);

	Schema_Object* RPCObject = Schema_AddObject(EndpointObject, Field);
	Payload.WriteToSchemaObject(RPCObject);

	Info.AddToSchema(EndpointObject, Field + 1);
}

} // namespace CrossServer
} // namespace SpatialGDK

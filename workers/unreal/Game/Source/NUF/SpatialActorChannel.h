// Copyright (c) Improbable Worlds Ltd, All Rights Reserved

#pragma once

#include <improbable/worker.h>

#include "Engine/ActorChannel.h"
#include "EntityId.h"
#include "SpatialOSCommandResult.h"
#include "Commander.h"
#include "improbable/worker.h"
#include "improbable/standard_library.h"
#include <map>
#include "SpatialTypeBinding.h"
#include "SpatialActorChannel.generated.h"

DECLARE_LOG_CATEGORY_EXTERN(LogSpatialOSActorChannel, Log, All);

// A replacement actor channel that plugs into the Engine's replication system and works with SpatialOS
UCLASS(Transient)
class NUF_API USpatialActorChannel : public UActorChannel
{
	GENERATED_BODY()

public:
	USpatialActorChannel(const FObjectInitializer & ObjectInitializer = FObjectInitializer::Get());

	// SpatialOS Entity ID.
	FORCEINLINE worker::EntityId GetEntityId() const
	{
		return ActorEntityId;
	}

	FORCEINLINE bool IsReadyForReplication() const
	{
		// Wait until we've reserved an entity ID.		
		return ActorEntityId != worker::EntityId{};
	}

	FORCEINLINE FPropertyChangeState GetChangeState(const TArray<uint16>& Changed) const
	{
		return{
			Changed,
			(uint8*)Actor,
			ActorReplicator->RepLayout->Cmds,
			ActorReplicator->RepLayout->BaseHandleToCmdIndex,
		};
	}
	
	void SendCreateEntityRequest(const TArray<uint16>& Changed);

	// UChannel interface
	virtual void Init(UNetConnection * connection, int32 channelIndex, bool bOpenedLocally) override;
	//NUF-sourcechange Requires virtual in ActorChannel.h
	virtual bool ReplicateActor() override;
	//NUF-sourcechange Requires virtual in ActorChannel.h
	virtual void SetChannelActor(AActor* InActor) override;

	// Distinguishes between channels created for actors that went through the "old" pipeline vs actors that are triggered through SpawnActor() calls.
	//In the future we may not use an actor channel for non-core actors.
	UPROPERTY(transient)
	bool bCoreActor;

	// Spatial update interop layer takes the outbunch created during ReplicateActor(), and parses it.
	// The problem is, if it's an initial send the whole actor & archetype data are serialized into this bunch before the header is written.
	// In the future, we probably want to parse that segment of data properly.
	// For now though, we keep track of where it ends so that we can skip there in SpatialUpdateInterop::SendSpatialUpdate().
	bool bSendingInitialBunch;

protected:
	// UChannel interface
	virtual bool CleanUp(const bool bForDestroy) override;
private:
	void BindToSpatialView();
	void UnbindFromSpatialView() const;

	void OnReserveEntityIdResponse(const worker::ReserveEntityIdResponseOp& Op);
	void OnCreateEntityResponse(const worker::CreateEntityResponseOp& Op);
	TWeakPtr<worker::Connection> WorkerConnection;
	TWeakPtr<worker::View> WorkerView;
	worker::EntityId ActorEntityId;

	worker::Dispatcher::CallbackKey ReserveEntityCallback;
	worker::Dispatcher::CallbackKey CreateEntityCallback;
	
	worker::RequestId<worker::ReserveEntityIdRequest> ReserveEntityIdRequestId;
	worker::RequestId<worker::CreateEntityRequest> CreateEntityRequestId;
};

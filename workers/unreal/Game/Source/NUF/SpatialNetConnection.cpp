// Copyright (c) Improbable Worlds Ltd, All Rights Reserved

#include "SpatialNetConnection.h"
#include "SpatialPackageMapClient.h"

USpatialNetConnection::USpatialNetConnection(const FObjectInitializer& ObjectInitializer) : Super(ObjectInitializer)
{
	InternalAck = 1;
}

void USpatialNetConnection::InitBase(UNetDriver* InDriver, class FSocket* InSocket, const FURL& InURL, EConnectionState InState, int32 InMaxPacket /*= 0*/, int32 InPacketOverhead /*= 0*/)
{
	Super::InitBase(InDriver, InSocket, InURL, InState, InMaxPacket, InPacketOverhead);

	auto PackageMapClient = NewObject<USpatialPackageMapClient>(this);
	PackageMapClient->Initialize(this, InDriver->GuidCache);

	PackageMap = PackageMapClient;
}

void USpatialNetConnection::InitLocalConnection(UNetDriver* InDriver, class FSocket* InSocket, const FURL& InURL, EConnectionState InState, int32 InMaxPacket, int32 InPacketOverhead)
{
	Super::InitLocalConnection(InDriver, InSocket, InURL, InState, InMaxPacket, InPacketOverhead);
}

void USpatialNetConnection::InitRemoteConnection(UNetDriver* InDriver, class FSocket* InSocket, const FURL& InURL, const class FInternetAddr& InRemoteAddr, EConnectionState InState, int32 InMaxPacket, int32 InPacketOverhead)
{
	Super::InitRemoteConnection(InDriver, InSocket, InURL, InRemoteAddr, InState, InMaxPacket, InPacketOverhead);
}

bool USpatialNetConnection::ClientHasInitializedLevelFor(const UObject* TestObject) const
{
	check(Driver->IsServer());
	return true;
	//Intentionally does not call Super::
}

void USpatialNetConnection::LowLevelSend(void * Data, int32 CountBytes, int32 CountBits)
{
	//Intentionally does not call Super::
}

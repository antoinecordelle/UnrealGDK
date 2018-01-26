#pragma once

#include "CoreMinimal.h"
#include "GameFramework/Actor.h"
#include "SpatialOSCommandResult.h"
#include "SpatialOSComponent.h"
#include "SpatialSpawner.generated.h"

class UPlayerSpawnerComponent;
class USpawnPlayerCommandResponder;
class SpawnPlayerResponse;

UCLASS()
class NUF_API ASpatialSpawner : public AActor
{
	GENERATED_BODY()
	
public:	
	ASpatialSpawner();

protected:
	virtual void BeginPlay() override;
	virtual void PostInitializeComponents() override;
	virtual void BeginDestroy() override;
	
	UFUNCTION()
	virtual void HandleSpawnRequest(USpawnPlayerCommandResponder* Responder);

	UPROPERTY()
	UPlayerSpawnerComponent* PlayerSpawnerComponent;
};

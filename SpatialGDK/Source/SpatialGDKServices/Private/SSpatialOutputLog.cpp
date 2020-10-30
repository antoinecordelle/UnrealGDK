// Copyright (c) Improbable Worlds Ltd, All Rights Reserved

#include "SSpatialOutputLog.h"

#include "Async/Async.h"
#include "DirectoryWatcherModule.h"
#include "Editor.h"
#include "Internationalization/Regex.h"
#include "Misc/CoreDelegates.h"
#include "Misc/FileHelper.h"
#include "Modules/ModuleManager.h"
#include "SlateOptMacros.h"
#include "SpatialGDKServicesConstants.h"
#include "SpatialGDKServicesModule.h"

#define LOCTEXT_NAMESPACE "SSpatialOutputLog"

DEFINE_LOG_CATEGORY(LogSpatialOutputLog);

static const FString LocalDeploymentLogsDir(FPaths::Combine(SpatialGDKServicesConstants::SpatialOSDirectory, TEXT("logs/localdeployment")));
static const FString LaunchLogFilename(TEXT("launch.log"));
static const float PollTimeInterval(0.05f);
auto ErrorLogFlagInfo = TTuple<bool, FString>(false, "");

void FArchiveLogFileReader::UpdateFileSize()
{
	Size = IFileManager::Get().GetStatData(*Filename).FileSize;
}

TUniquePtr<FArchiveLogFileReader> SSpatialOutputLog::CreateLogFileReader(const TCHAR* InFilename, uint32 Flags, uint32 BufferSize)
{
	IFileHandle* Handle = FPlatformFileManager::Get().GetPlatformFile().OpenRead(InFilename, !!(Flags & FILEREAD_AllowWrite));
	if (Handle == nullptr)
	{
		if (!(Flags & FILEREAD_NoFail))
		{
			UE_LOG(LogSpatialOutputLog, Error, TEXT("Failed to read file: %s"), InFilename);
		}

		return nullptr;
	}

	return MakeUnique<FArchiveLogFileReader>(Handle, InFilename, Handle->Size(), BufferSize);
}

BEGIN_SLATE_FUNCTION_BUILD_OPTIMIZATION
void SSpatialOutputLog::Construct(const FArguments& InArgs)
{
	SOutputLog::Construct(SOutputLog::FArguments());

	// Remove ourselves as the constructor of our parent (SOutputLog) added 'this' as a remote output device.
	GLog->RemoveOutputDevice(this);

	LogReader.Reset();
}
END_SLATE_FUNCTION_BUILD_OPTIMIZATION

void SSpatialOutputLog::ReadLatestLogFile()
{
	FString LatestLogDir;
	FDateTime LatestLogDirTime;

	// Go through all log directories in the spatial logs and find the most recently created (if one exists) and print the log file to the
	// Spatial Output.
	bool bGetLatestLogDir = IFileManager::Get().IterateDirectoryStat(
		*LocalDeploymentLogsDir, [&LatestLogDir, &LatestLogDirTime](const TCHAR* FileName, const FFileStatData& FileStats) {
			if (FileStats.bIsDirectory)
			{
				if (FileStats.CreationTime > LatestLogDirTime)
				{
					LatestLogDir = FString(FileName);
					LatestLogDirTime = FileStats.CreationTime;
				}
			}

			return true;
		});

	if (bGetLatestLogDir)
	{
		ResetPollingLogFile(FPaths::Combine(LatestLogDir, LaunchLogFilename));
	}
}

SSpatialOutputLog::~SSpatialOutputLog()
{
	CloseLogReader();

	ShutdownLogDirectoryWatcher(LocalDeploymentLogsDir);
}

void SSpatialOutputLog::StartUpLogDirectoryWatcher(const FString& LogDirectory)
{
	// This function will be called from the Slate thread and thus we must switch to the Game thread to create the Directory Watcher.
	AsyncTask(ENamedThreads::GameThread, [this, LogDirectory] {
		FDirectoryWatcherModule& DirectoryWatcherModule =
			FModuleManager::LoadModuleChecked<FDirectoryWatcherModule>(TEXT("DirectoryWatcher"));
		if (IDirectoryWatcher* DirectoryWatcher = DirectoryWatcherModule.Get())
		{
			// Watch the log directory for changes.
			if (!FPaths::DirectoryExists(LogDirectory))
			{
				UE_LOG(LogSpatialOutputLog, Log, TEXT("Spatial local deployment log directory '%s' does not exist. Will create it."),
					   *LogDirectory);

				if (!FPlatformFileManager::Get().GetPlatformFile().CreateDirectoryTree(*LogDirectory))
				{
					UE_LOG(
						LogSpatialOutputLog, Error,
						TEXT("Could not create the spatial local deployment log directory. The Spatial Output window will not function."));
					return;
				}
			}

			LogDirectoryChangedDelegate = IDirectoryWatcher::FDirectoryChanged::CreateRaw(this, &SSpatialOutputLog::OnLogDirectoryChanged);
			DirectoryWatcher->RegisterDirectoryChangedCallback_Handle(
				LogDirectory, LogDirectoryChangedDelegate, LogDirectoryChangedDelegateHandle,
				IDirectoryWatcher::WatchOptions::IncludeDirectoryChanges | IDirectoryWatcher::WatchOptions::IgnoreChangesInSubtree);
		}
	});
}

void SSpatialOutputLog::OnLogDirectoryChanged(const TArray<FFileChangeData>& FileChanges)
{
	// If this is a new folder creation then switch to watching the log files in that new log folder.
	for (const FFileChangeData& FileChange : FileChanges)
	{
		if (FileChange.Action == FFileChangeData::FCA_Added)
		{
#if PLATFORM_MAC
			// Unreal does not support IDirectoryWatcher::WatchOptions::IgnoreChangesInSubtree for macOS.
			// We need to double-check whether the current file really is a directory.
			if (!FPaths::DirectoryExists(FileChange.Filename))
			{
				continue;
			}
#endif
			// Now we can start reading the new log file in the new log folder.
			ResetPollingLogFile(FPaths::Combine(FileChange.Filename, LaunchLogFilename));
			return;
		}
	}
}

void SSpatialOutputLog::OnClearLog()
{
	// SOutputLog will clear the messages and the SelectedLogCategories.
	SOutputLog::OnClearLog();

	// Clear the AvailableLogCategories and SelectedLogCategories as we generate many worker categories which are hard to parse.
	Filter.AvailableLogCategories.Reset();
	Filter.SelectedLogCategories.Reset();
}

void SSpatialOutputLog::ShutdownLogDirectoryWatcher(const FString& LogDirectory)
{
	AsyncTask(ENamedThreads::GameThread, [LogDirectory, LogDirectoryChangedDelegateHandle = LogDirectoryChangedDelegateHandle] {
		FDirectoryWatcherModule& DirectoryWatcherModule =
			FModuleManager::LoadModuleChecked<FDirectoryWatcherModule>(TEXT("DirectoryWatcher"));
		if (IDirectoryWatcher* DirectoryWatcher = DirectoryWatcherModule.Get())
		{
			DirectoryWatcher->UnregisterDirectoryChangedCallback_Handle(LogDirectory, LogDirectoryChangedDelegateHandle);
		}
	});
}

void SSpatialOutputLog::CloseLogReader()
{
	if (GEditor != nullptr)
	{
		// Delete the old timer if one exists.
		GEditor->GetTimerManager()->ClearTimer(PollTimer);
	}

	FScopeLock CloseLock(&LogReaderMutex);

	// Clean up the the previous file reader if it existed.
	if (LogReader.IsValid())
	{
		LogReader->Close();
		LogReader = nullptr;
	}
}

void SSpatialOutputLog::ResetPollingLogFile(const FString& LogFilePath)
{
	CloseLogReader();

	FScopeLock CreateLock(&LogReaderMutex);

	// FILEREAD_AllowWrite is required as we must match the permissions of the other processes writing to our log file in order to read from
	// it.
	LogReader = CreateLogFileReader(*LogFilePath, FILEREAD_AllowWrite, PLATFORM_FILE_READER_BUFFER_SIZE);

	if (LogReader.IsValid())
	{
		PollLogFile(LogFilePath);
	}
	else
	{
		UE_LOG(LogSpatialOutputLog, Error, TEXT("Could not set up log file reader for %s"), *LogFilePath);
	}
}

void SSpatialOutputLog::PollLogFile(const FString& LogFilePath)
{
	// Poll log files in a background thread since we are doing a lot of string operations.
	AsyncTask(ENamedThreads::AnyBackgroundThreadNormalTask, [this, LogFilePath] {
		FScopeLock PollLock(&LogReaderMutex);

		if (!LogReader.IsValid())
		{
			UE_LOG(LogSpatialOutputLog, Error, TEXT("Attempted to read from log file but LogReader is not valid."));
			return;
		}

		FScopedLoadingState ScopedLoadingState(*LogFilePath);

		// Find out the current size of the log file. This is a cheaper operation than opening a new file reader on every poll.
		LogReader->UpdateFileSize();

		const int32 SizeDifference = LogReader->TotalSize() - LogReader->Tell();

		// New log lines have been added, serialize them.
		if (SizeDifference > 0)
		{
			uint8* Ch = static_cast<uint8*>(FMemory::Malloc(SizeDifference));

			LogReader->Serialize(Ch, SizeDifference);

			FString ReadResult;
			FFileHelper::BufferToString(ReadResult, Ch, SizeDifference);

			TArray<FString> LogLines;

			// All log lines begin with 'time='. We use this as our log line delimiter.
			ReadResult.ParseIntoArray(LogLines, TEXT("time="), true);

			for (const FString& LogLine : LogLines)
			{
				FormatAndPrintRawLogLine(LogLine);
			}

			FMemory::Free(Ch);
		}

		StartPollTimer(LogFilePath);
	});
}

void SSpatialOutputLog::StartPollTimer(const FString& LogFilePath)
{
	// Start a timer to read the log file every PollTimeInterval seconds
	// Timers must be started on the game thread.
	AsyncTask(ENamedThreads::GameThread, [this, LogFilePath] {
		// It's possible that GEditor won't exist when shutting down.
		if (GEditor != nullptr)
		{
			GEditor->GetTimerManager()->SetTimer(
				PollTimer,
				[this, LogFilePath]() {
					PollLogFile(LogFilePath);
				},
				PollTimeInterval, false);
		}
	});
}

void SSpatialOutputLog::FormatAndPrintRawErrorLine(const FString& LogLine)
{
	FString LogCategory;

	if (ErrorLogFlagInfo.Key)
	{
		LogCategory = ErrorLogFlagInfo.Value;
	}

	// Serialization must be done on the game thread.
	AsyncTask(ENamedThreads::GameThread, [this, LogLine, LogCategory] {
		Serialize(*LogLine, ELogVerbosity::Error, FName(*LogCategory));
	});
}

void SSpatialOutputLog::FormatAndPrintRawLogLine(const FString& LogLine)
{
	const FRegexPattern LogPattern = FRegexPattern(TEXT("\\[(\\w*)\\] \\[(\\w*)\\] (.*)"));
	FRegexMatcher LogMatcher(LogPattern, LogLine);

	if (!LogMatcher.FindNext())
	{
		// If this log line did not match the log line regex then it is an error line which is parsed differently.
		FormatAndPrintRawErrorLine(LogLine);
		return;
	}
	FString LogCategory = LogMatcher.GetCaptureGroup(1);
	FString LogLevelText = LogMatcher.GetCaptureGroup(2);
	FString LogMessage = LogMatcher.GetCaptureGroup(3);

	const FRegexPattern WorkerLogPattern = FRegexPattern(TEXT("\\[(.*)\\] (.*)"));
	FRegexMatcher WorkerLogMatcher(WorkerLogPattern, LogMessage);

	if (WorkerLogMatcher.FindNext())
	{
		FString LogMessageCategory = WorkerLogMatcher.GetCaptureGroup(1);
		LogMessage = WorkerLogMatcher.GetCaptureGroup(2);
		LogCategory = LogMessageCategory.Left(20);
	}
	else
	{
		// If the Log Category is not of type Worker, then it should be categorised as Runtime instead
		LogCategory = TEXT("Runtime");
	}

	ELogVerbosity::Type LogVerbosity = ELogVerbosity::Display;
	ErrorLogFlagInfo.Key = false;

	if (LogLevelText.Contains(TEXT("error")))
	{
		LogVerbosity = ELogVerbosity::Error;
		ErrorLogFlagInfo.Key = true;
		ErrorLogFlagInfo.Value = LogCategory;
	}
	else if (LogLevelText.Contains(TEXT("warn")))
	{
		LogVerbosity = ELogVerbosity::Warning;
	}
	else if (LogLevelText.Contains(TEXT("debug")))
	{
		LogVerbosity = ELogVerbosity::Verbose;
	}
	else if (LogLevelText.Contains(TEXT("trace")))
	{
		LogVerbosity = ELogVerbosity::Verbose;
	}
	else
	{
		LogVerbosity = ELogVerbosity::Log;
	}

	// Serialization must be done on the game thread.
	AsyncTask(ENamedThreads::GameThread, [this, LogMessage, LogVerbosity, LogCategory] {
	Serialize(*LogMessage, LogVerbosity, FName(*LogCategory));
	});
}

#undef LOCTEXT_NAMESPACE

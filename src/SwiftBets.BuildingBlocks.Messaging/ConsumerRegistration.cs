namespace SwiftBets.BuildingBlocks.Messaging;

/// <param name="TopicBase">The topic, without the environment suffix.</param>
/// <param name="GroupId">The consumer group.</param>
/// <param name="StartAtLatest">A new group starts at the end of the topic instead of the beginning: for observers that only care about what happens from now on.</param>
/// <param name="MaxInFlight">Messages handled at once. 1 handles them one by one; more runs messages with different keys concurrently, keeps each key in order, and commits each partition's offset only past a contiguous run of handled messages.</param>
public sealed record ConsumerRegistration(string TopicBase, string GroupId, bool StartAtLatest = false, int MaxInFlight = 1);

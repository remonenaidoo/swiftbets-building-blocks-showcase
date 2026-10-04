WITH next AS
(
    SELECT TOP (@BatchSize) *
    -- Seek the filtered pending index: with a growing sent history the optimizer otherwise scans the whole table (D190).
    FROM outbox.Messages WITH (INDEX(IX_OutboxMessages_Pending), UPDLOCK, READPAST, ROWLOCK)
    WHERE SentAt IS NULL
      AND NextAttemptAt <= @Now
      AND (LeaseUntil IS NULL OR LeaseUntil < @Now)
    ORDER BY Sequence
)
UPDATE next
SET LeaseOwner = @Owner, LeaseUntil = @LeaseUntil
OUTPUT inserted.Sequence, inserted.Id, inserted.Topic, inserted.MessageKey, inserted.Payload, inserted.Headers, inserted.AttemptCount;

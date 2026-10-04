-- Counted from the filtered pending index, never the whole table (D190).
SELECT COUNT_BIG(*) FROM outbox.Messages WITH (INDEX(IX_OutboxMessages_Pending)) WHERE SentAt IS NULL;

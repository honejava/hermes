package pl.allegro.tech.hermes.consumers.consumer.offset;

public interface NewMessageCommitter {
    FailedToCommitOffsets commitOffsets(OffsetsToCommit offsetsToCommit);
}

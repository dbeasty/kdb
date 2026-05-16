package dev.kdb.compaction

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag

public interface CompactionCoordinator {
    public suspend fun broadcastIntent(intent: CompactionIntent): CompactionAckSet
}

public class InProcessCompactionCoordinator : CompactionCoordinator {
    private val peerHeads = mutableMapOf<String, KdbHash>()

    public fun updatePeerHead(
        peerId: String,
        head: KdbHash,
    ) {
        peerHeads[peerId] = head
    }

    public fun clearPeers() {
        peerHeads.clear()
    }

    public fun peerHeads(): Set<KdbHash> = peerHeads.values.toSet()

    override suspend fun broadcastIntent(intent: CompactionIntent): CompactionAckSet {
        val rejected = mutableMapOf<String, KdbHash>()
        for ((peer, head) in peerHeads) {
            if (head != intent.boundary) {
                rejected[peer] = head
            }
        }
        val acked = peerHeads.keys.filter { it !in rejected }.toSet()
        return CompactionAckSet(acked, rejected)
    }
}

public class CompactionPeerRegistry(
    private val coordinator: InProcessCompactionCoordinator,
) {
    public fun updatePeerHeads(
        namespaceId: String,
        heads: Map<String, KdbHash>,
    ) {
        coordinator.clearPeers()
        for ((peer, head) in heads) {
            coordinator.updatePeerHead(peer, head)
        }
    }
}

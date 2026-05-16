package dev.kdb.index

import dev.kdb.dag.CommitDag
import dev.kdb.index.btree.btreeIndexStoreFactory
import dev.kdb.index.fulltext.fullTextIndexStoreFactory
import dev.kdb.index.hash.hashIndexStoreFactory
import dev.kdb.index.vector.vectorIndexStoreFactory
import dev.kdb.storage.StorageAdapter

/**
 * Production [IndexStoreFactory] wiring Layer 5 index implementations (Component 12–14).
 */
public class CompositeIndexStoreFactory(
    dag: CommitDag,
    storage: StorageAdapter,
    private val vectorDimensions: Int = 128,
    private val hashFactory: IndexStoreFactory = hashIndexStoreFactory(dag, storage),
    private val btreeFactory: IndexStoreFactory = btreeIndexStoreFactory(dag, storage),
    private val fullTextFactory: IndexStoreFactory = fullTextIndexStoreFactory(dag, storage),
    private val vectorFactory: IndexStoreFactory = vectorIndexStoreFactory(dag, storage, vectorDimensions),
) : IndexStoreFactory {

    override fun create(descriptor: IndexDescriptor): IndexStore =
        when (descriptor.type) {
            IndexType.HASH -> hashFactory.create(descriptor)
            IndexType.BTREE -> btreeFactory.create(descriptor)
            IndexType.FULLTEXT -> fullTextFactory.create(descriptor)
            IndexType.VECTOR -> vectorFactory.create(descriptor)
        }
}

public fun compositeIndexStoreFactory(
    dag: CommitDag,
    storage: StorageAdapter,
    vectorDimensions: Int = 128,
): IndexStoreFactory = CompositeIndexStoreFactory(dag, storage, vectorDimensions)

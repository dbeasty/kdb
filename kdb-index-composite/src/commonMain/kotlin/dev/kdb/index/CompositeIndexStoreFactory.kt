package dev.kdb.index

import dev.kdb.dag.CommitDag
import dev.kdb.index.btree.btreeIndexStoreFactory
import dev.kdb.index.fulltext.fullTextIndexStoreFactory
import dev.kdb.index.hash.hashIndexStoreFactory
import dev.kdb.index.vector.vectorIndexStoreFactory
import dev.kdb.storage.StorageAdapter

/**
 * Production [IndexStoreFactory] wiring Layer 5 index implementations (Component 12–14).
 *
 * [vectorDimensions] is only the fallback: a VECTOR descriptor's `dimensions` / `metric` / `m` /
 * `ef_construction` / `ef_search` options override it (Layer 16 §7, §9.2). FULLTEXT and VECTOR
 * stores persist their snapshots through [blobs]; share one [IndexBlobStore] between this
 * factory and [indexManager] so the catalog and the snapshots land in the same place.
 */
public class CompositeIndexStoreFactory(
    dag: CommitDag,
    storage: StorageAdapter,
    private val vectorDimensions: Int = 128,
    private val hashFactory: IndexStoreFactory = hashIndexStoreFactory(dag, storage),
    private val btreeFactory: IndexStoreFactory = btreeIndexStoreFactory(dag, storage),
    blobs: IndexBlobStore = storageAdapterIndexBlobStore(storage),
    private val fullTextFactory: IndexStoreFactory = fullTextIndexStoreFactory(dag, storage, blobs),
    private val vectorFactory: IndexStoreFactory = vectorIndexStoreFactory(dag, storage, vectorDimensions, blobs = blobs),
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
    blobs: IndexBlobStore = storageAdapterIndexBlobStore(storage),
): IndexStoreFactory = CompositeIndexStoreFactory(dag, storage, vectorDimensions, blobs = blobs)

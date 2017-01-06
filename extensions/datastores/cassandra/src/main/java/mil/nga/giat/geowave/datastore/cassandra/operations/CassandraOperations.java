package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.futures.CompletableFuturesExtra;

import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow.CassandraField;
import mil.nga.giat.geowave.datastore.cassandra.CassandraWriter;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraOptions;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;
import mil.nga.giat.geowave.datastore.cassandra.util.SessionPool;

public class CassandraOperations implements
		DataStoreOperations
{
	private final Session session;
	private final String gwNamespace;
	private final static int WRITE_RESPONSE_THREAD_SIZE = 16;
	private final static int READ_RESPONSE_THREAD_SIZE = 16;
	protected final static ExecutorService WRITE_RESPONSE_THREADS = MoreExecutors.getExitingExecutorService(
			(ThreadPoolExecutor) Executors.newFixedThreadPool(
					WRITE_RESPONSE_THREAD_SIZE));
	protected final static ExecutorService READ_RESPONSE_THREADS = MoreExecutors.getExitingExecutorService(
			(ThreadPoolExecutor) Executors.newFixedThreadPool(
					READ_RESPONSE_THREAD_SIZE));
	private static final Object CREATE_TABLE_MUTEX = new Object();
	private final Map<String, PreparedStatement> preparedRangeReadsPerTable = new HashMap<>();
	private final Map<String, PreparedStatement> preparedRowReadPerTable = new HashMap<>();
	private final Map<String, PreparedStatement> preparedWritesPerTable = new HashMap<>();
	private static Map<String, Boolean> tableExistsCache = new HashMap<>();

	private final CassandraOptions options;

	public CassandraOperations(
			final CassandraRequiredOptions options ) {
		if ((options.getGeowaveNamespace() == null) || options.getGeowaveNamespace().equals(
				"")) {
			gwNamespace = "default";
		}
		else {
			gwNamespace = options.getGeowaveNamespace();
		}
		session = SessionPool.getInstance().getSession(
				options.getContactPoint());
		// TODO consider exposing important keyspace options through commandline
		// such as understanding how to properly enable cassandra in production
		// - with data centers and snitch, for now because this is only creating
		// a keyspace "if not exists" a user can create a keyspace matching
		// their geowave namespace with any settings they want manually
		session.execute(
				SchemaBuilder
						.createKeyspace(
								gwNamespace)
						.ifNotExists()
						.with()
						.replication(
								ImmutableMap.of(
										"class",
										"SimpleStrategy",
										"replication_factor",
										options.getAdditionalOptions().getReplicationFactor()))
						.durableWrites(
								options.getAdditionalOptions().isDurableWrites()));
		this.options = options.getAdditionalOptions();
	}

	@Override
	public boolean tableExists(
			final String tableName ) {
		Boolean tableExists = tableExistsCache.get(
				tableName);
		if (tableExists == null) {
			final KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(
					gwNamespace);
			if (keyspace != null) {
				tableExists = keyspace.getTable(
						tableName) != null;
			}
			else {
				tableExists = false;
			}
			tableExistsCache.put(
					tableName,
					tableExists);
		}
		return tableExists;
	}

	public Session getSession() {
		return session;
	}

	public Create getCreateTable(
			final String table ) {
		return SchemaBuilder.createTable(
				gwNamespace,
				table).ifNotExists();
	}

	public void executeCreateTable(
			final Create create,
			final String tableName ) {
		System.out.println(
				"create " + tableName);
		System.out.println(
				"create " + create.getQueryString());
		session.execute(
				create);
		tableExistsCache.put(
				tableName,
				true);
	}

	public Insert getInsert(
			final String table ) {
		System.out.println(
				"insert " + table);
		return QueryBuilder.insertInto(
				gwNamespace,
				table);
	}

	public Select getSelect(
			final String table,
			final String... columns ) {
		return (columns.length == 0 ? QueryBuilder.select() : QueryBuilder.select(
				columns)).from(
						gwNamespace,
						table);
	}

	public BaseDataStoreOptions getOptions() {
		return options;
	}

	public BatchedWrite getBatchedWrite(
			final String tableName ) {
		PreparedStatement preparedWrite;
		synchronized (preparedWritesPerTable) {
			preparedWrite = preparedWritesPerTable.get(
					tableName);
			if (preparedWrite == null) {
				final Insert insert = getInsert(
						tableName);
				for (final CassandraField f : CassandraField.values()) {
					insert.value(
							f.getFieldName(),
							QueryBuilder.bindMarker(
									f.getBindMarkerName()));
				}
				System.err.println(insert.getQueryString());
				preparedWrite = session.prepare(
						insert);
				preparedWritesPerTable.put(
						tableName,
						preparedWrite);
			}
		}
		return new BatchedWrite(
				session,
				preparedWrite,
				options.getBatchWriteSize());
	}

	public BatchedRangeRead getBatchedRangeRead(
			final String tableName,
			final List<ByteArrayRange> ranges ) {
		PreparedStatement preparedRead;
		synchronized (preparedRangeReadsPerTable) {
			preparedRead = preparedRangeReadsPerTable.get(
					tableName);
			if (preparedRead == null) {
				final Select select = getSelect(
						tableName);
				select
						.where(
								QueryBuilder.eq(
										CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
										QueryBuilder.bindMarker(
												CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName())))
						.and(
								QueryBuilder.gte(
										CassandraRow.CassandraField.GW_IDX_KEY.getFieldName(),
										QueryBuilder.bindMarker(
												CassandraRow.CassandraField.GW_IDX_KEY.getLowerBoundBindMarkerName())))
						.and(
								QueryBuilder.lt(
										CassandraRow.CassandraField.GW_IDX_KEY.getFieldName(),
										QueryBuilder.bindMarker(
												CassandraRow.CassandraField.GW_IDX_KEY.getUpperBoundBindMarkerName())));
				preparedRead = session.prepare(
						select);
				preparedRangeReadsPerTable.put(
						tableName,
						preparedRead);
			}
		}

		return new BatchedRangeRead(
				preparedRead,
				this,
				ranges);
	}

	public BatchedRangeRead getBatchedRangeRead(
			final String tableName ) {
		return getBatchedRangeRead(
				tableName,
				new ArrayList<>());
	}

	public RowRead getRowRead(
			final String tableName,
			final byte[] rowIdx ) {
		PreparedStatement preparedRead;
		synchronized (preparedRowReadPerTable) {
			preparedRead = preparedRowReadPerTable.get(
					tableName);
			if (preparedRead == null) {
				final Select select = getSelect(
						tableName);
				select
						.where(
								QueryBuilder.eq(
										CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
										QueryBuilder.bindMarker(
												CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName())))
						.and(
								QueryBuilder.eq(
										CassandraRow.CassandraField.GW_IDX_KEY.getFieldName(),
										QueryBuilder.bindMarker(
												CassandraRow.CassandraField.GW_IDX_KEY.getBindMarkerName())));
				preparedRead = session.prepare(
						select);
				preparedRowReadPerTable.put(
						tableName,
						preparedRead);
			}
		}

		return new RowRead(
				preparedRead,
				this,
				rowIdx);

	}

	public RowRead getRowRead(
			final String tableName ) {
		return getRowRead(
				tableName,
				null);
	}

	// public CloseableIterator<CassandraRow> executeQuery(
	// final Statement... statements ) {
	// // first create a list of asynchronous query executions
	// final List<ResultSetFuture> futures = Lists.newArrayListWithExpectedSize(
	// statements.length);
	// for (final Statement s : statements) {
	// futures.add(
	// session.executeAsync(
	// s));
	// }
	// for (ResultSetFuture f : futures) {
	// Futures.addCallback(
	// f,
	// new IngestCallback(),
	// CassandraOperations.WRITE_RESPONSE_THREADS);
	// }
	// // convert the list of futures to an asynchronously as completed
	// // iterator on cassandra rows
	// final
	// com.aol.cyclops.internal.react.stream.CloseableIterator<CassandraRow>
	// results = new LazyReact()
	// .fromStreamFutures(
	// Lists.transform(
	// futures,
	// new ListenableFutureToCompletableFuture()).stream())
	// .flatMap(
	// r -> StreamSupport.stream(
	// r.spliterator(),
	// false))
	// .map(
	// r -> new CassandraRow(
	// r))
	// .iterator();
	// // now convert cyclops-react closeable iterator to a geowave closeable
	// // iterator
	// return new CloseableIteratorWrapper<CassandraRow>(
	// new Closeable() {
	//
	// @Override
	// public void close()
	// throws IOException {
	// results.close();
	// }
	// },
	// results);
	// }
	public CloseableIterator<CassandraRow> executeQuery(
			final Statement... statements ) {
		final List<CassandraRow> rows = new ArrayList<>();
		for (final Statement s : statements) {
			final ResultSet r = session.execute(
					s);
			for (final Row row : r) {
				rows.add(
						new CassandraRow(
								row));
			}
		}
		return new CloseableIterator.Wrapper<CassandraRow>(
				rows.iterator());
	}

	public Writer createWriter(
			final String tableName,
			final boolean createTable ) {
		final CassandraWriter writer = new CassandraWriter(
				tableName,
				this);
		if (createTable) {
			synchronized (CREATE_TABLE_MUTEX) {
				if (!tableExists(
						tableName)) {
					final Create create = getCreateTable(
							tableName);
					for (final CassandraField f : CassandraField.values()) {
						f.addColumn(
								create);
					}
					executeCreateTable(
							create,
							tableName);
				}
			}
		}
		return writer;
	}

	@Override
	public void deleteAll()
			throws Exception {}

	private static class ListenableFutureToCompletableFuture implements
			Function<ListenableFuture<ResultSet>, CompletableFuture<ResultSet>>
	{
		@Override
		public CompletableFuture<ResultSet> apply(
				final ListenableFuture<ResultSet> input ) {
			return CompletableFuturesExtra.toCompletableFuture(
					input);
		}
	}
}

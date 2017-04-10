package mil.nga.giat.geowave.mapreduce.copy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.mapreduce.HadoopWritableSerializationTool;
import mil.nga.giat.geowave.mapreduce.HadoopWritableSerializer;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

/**
 * A basic implementation of copy as a reducer
 */
public class StoreCopyReducer extends
		Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>
{
	private AdapterIndexMappingStore store;
	private HadoopWritableSerializationTool serializationTool;

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		store = GeoWaveOutputFormat.getJobContextAdapterIndexMappingStore(context);
		serializationTool = new HadoopWritableSerializationTool(
				GeoWaveInputFormat.getJobContextAdapterStore(context));
	}

	@Override
	protected void reduce(
			final GeoWaveInputKey key,
			final Iterable<ObjectWritable> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		final HadoopWritableSerializer<?, Writable> serializer = serializationTool
				.getHadoopWritableSerializerForAdapter(key.getAdapterId());
		
		final Iterable<Object> transformedValues = Iterables.transform(
				values,
				new Function<ObjectWritable, Object>() {
					@Override
					public Object apply(
							final ObjectWritable writable ) {
						final Object innerObj = writable.get();
						return (innerObj instanceof Writable) ? serializer.fromWritable((Writable) innerObj) : innerObj;
					}
				});


		final Iterator<Object> objects = transformedValues.iterator();
		while (objects.hasNext()) {
			final AdapterToIndexMapping mapping = store.getIndicesForAdapter(key.getAdapterId());
			context.write(
					new GeoWaveOutputKey<>(
							mapping.getAdapterId(),
							Arrays.asList(mapping.getIndexIds())),
					objects.next());
		}
	}

}

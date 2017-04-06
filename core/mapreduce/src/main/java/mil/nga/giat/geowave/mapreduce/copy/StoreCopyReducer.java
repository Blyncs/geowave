package mil.nga.giat.geowave.mapreduce.copy;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

/**
 * A basic implementation of copy as a reducer
 */
public class StoreCopyReducer extends
		Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>
{

	@Override
	protected void reduce(
			final GeoWaveInputKey key,
			final Iterable<ObjectWritable> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		final Iterator<ObjectWritable> objects = values.iterator();
		if (objects.hasNext()) {
			context.write(
					key,
					objects.next());
		}
	}

}

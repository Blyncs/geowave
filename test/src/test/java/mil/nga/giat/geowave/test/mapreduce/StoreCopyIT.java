package mil.nga.giat.geowave.test.mapreduce;

import java.io.File;
import java.net.URISyntaxException;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import mil.nga.giat.geowave.adapter.raster.util.ZipUtils;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.operations.CopyCommand;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.GeoWaveBasicSpatialVectorIT;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.MAP_REDUCE
})
@GeoWaveTestStore({
	GeoWaveStoreType.ACCUMULO,
	GeoWaveStoreType.HBASE
})
public class StoreCopyIT
{
	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TestUtils.TEST_RESOURCE_PACKAGE + "basic-testdata.zip";
	protected static final String HAIL_TEST_CASE_PACKAGE = TestUtils.TEST_CASE_BASE + "hail_test_case/";
	protected static final String HAIL_SHAPEFILE_FILE = HAIL_TEST_CASE_PACKAGE + "hail.shp";

	protected DataStorePluginOptions outputDataStorePluginOptions;
	protected DataStorePluginOptions inputDataStorePluginOptions;

	private final static Logger LOGGER = Logger.getLogger(
			StoreCopyIT.class);
	private static long startMillis;

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		ZipUtils.unZipFile(
				new File(
						GeoWaveBasicSpatialVectorIT.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TestUtils.TEST_CASE_BASE);

		startMillis = System.currentTimeMillis();
		LOGGER.warn(
				"-----------------------------------------");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"*         RUNNING StoreCopyIT           *");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn(
				"-----------------------------------------");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"*      FINISHED StoreCopyIT             *");
		LOGGER.warn(
				"*         " + ((System.currentTimeMillis() - startMillis) / 1000) + "s elapsed.                 *");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"-----------------------------------------");
	}

	@Test
	public void testStoreCopy()
			throws Exception {
		// Load some test data
		TestUtils.testLocalIngest(
				inputDataStorePluginOptions,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		// final MapReduceTestEnvironment env = MapReduceTestEnvironment.getInstance();

		// Set up the copy command
		final CopyCommand command = new CopyCommand();

		// We're going to override these anyway.
		command.setParameters(
				null,
				null);

		command.setInputStoreOptions(
				outputDataStorePluginOptions);
		command.setOutputStoreOptions(
				outputDataStorePluginOptions);

		command.getOptions().setMinSplits(
				MapReduceTestUtils.MIN_INPUT_SPLITS);
		command.getOptions().setMaxSplits(
				MapReduceTestUtils.MAX_INPUT_SPLITS);
		command.getOptions().setNumThreads(
				8);

		ToolRunner.run(
				command.createRunner(
						new ManualOperationParams()),
				new String[] {});
		
		// TODO: load/query the copy store
	}
}

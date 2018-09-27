package debs2018;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import marmot.GeometryColumnInfo;
import marmot.MarmotServer;
import marmot.Plan;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SquareGrid;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DrawGridCellMain implements Runnable {
	private static final String RESULT = "debs/empty_grid_cells";
	
	private final MarmotServer m_marmot;
	
	private DrawGridCellMain(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		try {
			Size2d cellSize = GeoClientUtils.divide(Globals.BOUNDS, Globals.RESOLUTION);
			
			Plan plan = m_marmot.planBuilder("draw_grid_cell")
								.loadSquareGridFile(new SquareGrid(Globals.BOUNDS, cellSize), -1)
								.store(RESULT)
								.build();
			GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:4326");
			m_marmot.createDataSet(RESULT, plan, GEOMETRY(gcInfo), FORCE);
		}
		catch ( Exception e ) {
			e.printStackTrace(System.err);
			return;
		}
	}
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("").stopAtNonOption(true);
		parser.addOption("hadoop", "execute this job at Hadoop cluster or not");
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("hadoop") ) {
			int exitCode = ToolRunner.run(new Driver(), cl.getArgumentAll());
			System.exit(exitCode);
		}
		else {
			StopWatch watch = StopWatch.start();

			MarmotServer marmot = MarmotServer.initializeForLocalhost();
			new DrawGridCellMain(marmot).run();
			
			System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
		}
	}
	
	public static class Driver extends Configured implements Tool {
		@Override
		public int run(String[] args) throws Exception {
			try {
				MarmotServer marmot = MarmotServer.initialize(getConf());
				marmot.setMapOutputCompression(true);

				StopWatch watch = StopWatch.start();
				new DrawGridCellMain(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

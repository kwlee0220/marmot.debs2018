package debs2018;

import static marmot.optor.AggregateFunction.COUNT;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotServer;
import marmot.Plan;
import marmot.geo.GeoClientUtils;
import marmot.protobuf.PBUtils;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildHistogram implements Runnable {
	private final MarmotServer m_marmot;
	
	private BuildHistogram(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		try {
			String prjExpr = "the_geom,departure_port,ship_id,ship_type,"
							+ "speed,course,heading,ts";
			String initExpr = "$pattern = ST_DTPattern('dd-MM-yy HH:mm:ss')";
			String expr = "ts = ST_DTToMillis(ST_DTParseLE(timestamp, $pattern))";
			Size2d cellSize = GeoClientUtils.divide(Globals.BOUNDS, Globals.RESOLUTION);
			
			ShipTrajectoryGenerator trjGen = new ShipTrajectoryGenerator();
			Plan plan = m_marmot.planBuilder("build_ship_trajectory")
								.load(Globals.SHIP_TRACKS)
								.expand("ts:long", initExpr, expr)
								.project(prjExpr)
								.groupBy("ship_id")
									.taggedKeyColumns("ship_type")
									.orderBy("ts:A")
									.apply(PBUtils.serializeJava(trjGen))
								.filter("dest_port != null")
								.assignSquareGridCell("the_geom", Globals.BOUNDS, cellSize)
								.groupBy("cell_id,depart_port,dest_port")
									.taggedKeyColumns("cell_pos")
									.aggregate(COUNT().as("count"))
								.expand("x:int,y:int", "x = cell_pos.x; y=cell_pos.y;")
								.project("x,y,depart_port,dest_port,count")
								.store(Globals.SHIP_GRID_CELLS)
								.build();
			m_marmot.createDataSet(Globals.SHIP_GRID_CELLS, plan, true);
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
			new BuildHistogram(marmot).run();
			
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
				new BuildHistogram(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

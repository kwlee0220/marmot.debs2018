package debs2018;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

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
public class BuildTrajectories implements Runnable {
	private final MarmotServer m_marmot;
	
	private BuildTrajectories(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		try {
			String prjExpr = "the_geom,departure_port,ship_id,ship_type,"
							+ "speed,course,heading,ts";
			String initExpr = "$pattern = ST_DTPattern('dd-MM-yy HH:mm:ss')";
			String expr = "ts = ST_DTToMillis(ST_DTParseLE(timestamp, $pattern))";
			
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
								.store(Globals.SHIP_TRACKS_LABELED)
								.build();
			m_marmot.createDataSet(Globals.SHIP_TRACKS_LABELED, plan, true);
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
			new BuildTrajectories(marmot).run();
			
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
				new BuildTrajectories(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

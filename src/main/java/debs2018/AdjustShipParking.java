package debs2018;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import marmot.MarmotServer;
import marmot.Plan;
import marmot.protobuf.PBUtils;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AdjustShipParking implements Runnable {
	private final MarmotServer m_marmot;
	
	private AdjustShipParking(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		try {
			String initExpr = "$pat1 = ST_DTPattern('dd-MM-yy H:mm:ss');"
							+ "$pat2 = ST_DTPattern('dd-MM-yy H:mm');";
			String expr = "if ( timestamp.split(':').length == 2 ) {"
						+ "   ts = ST_DTToMillis(ST_DTParseLE(timestamp, $pat2)); }"
						+ "else { ts = ST_DTToMillis(ST_DTParseLE(timestamp, $pat1)); }";
			String expr2 = "if ( arrival_calc.split(':').length == 2 ) {"
						+ "   arrival_calc = ST_DTToMillis(ST_DTParseLE(arrival_calc, $pat2)); }"
						+ "else { arrival_calc = ST_DTToMillis(ST_DTParseLE(arrival_calc, $pat1)); }"; 
			
			ShipTrajectoryAdjust adjust = new ShipTrajectoryAdjust();
			Plan plan = m_marmot.planBuilder("build_ship_trajectory")
								.load(Globals.SHIP_TRACKS_TIME)
								.filter("arrival_port_calc != null && arrival_port_calc.length() > 0 ")
								.expand("ts:long")
									.initializer(initExpr, expr)
								.expand("arrival_calc:long")
									.initializer(initExpr, expr2)
								.groupBy("ship_id,departure_port,arrival_port_calc")
									.orderBy("ts:A")
									.apply(PBUtils.serializeJava(adjust))
								.store(Globals.SHIP_TRACKS_TIME_ADJUST)
								.build();
			m_marmot.createDataSet(Globals.SHIP_TRACKS_TIME_ADJUST, plan, true);
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
			new AdjustShipParking(marmot).run();
			
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
				new AdjustShipParking(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

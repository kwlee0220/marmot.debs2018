package debs2018;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.MarmotServer;
import marmot.Plan;
import marmot.PlanExecutionMode;
import marmot.RecordSet;
import marmot.protobuf.PBUtils;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssignGridCell implements Runnable {
	private final MarmotServer m_marmot;
	
	private AssignGridCell(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		try {
			DataSet input = m_marmot.getDataSet(Globals.SHIP_TRACKS);
			String geomCol = input.getGeometryColumn();
			String srid = input.getSRID();
			
			String prjExpr = String.format("%s,ship_id,departure_port_name as depart_port,ts", geomCol);
			String initExpr = "$pattern = ST_DTPattern('dd-MM-yy HH:mm:ss')";
			String expr = "ts = ST_DTToMillis(ST_DTParseLE(timestamp, $pattern))";
			
			ShipTrajectoryGenerator trjGen = new ShipTrajectoryGenerator();
			
			Plan plan = m_marmot.planBuilder("build_ship_trajectory")
								.setExecutionMode(PlanExecutionMode.MAP_REDUCE)
								.load(Globals.SHIP_TRACKS)
								.expand("ts:long", initExpr, expr)
								.project(prjExpr)
								.groupBy("ship_id")
										.orderBy("ts:A")
										.flatTransform(PBUtils.serializeJava(trjGen))
								.expand("the_geom:line_string","the_geom = trajectory.lineString")
								.store(Globals.TEMP_SHIP_TRJ)
								.build();
			DataSet result = m_marmot.createDataSet(Globals.TEMP_SHIP_TRJ, plan, true);
			
			RecordSet output = new ShipTrajRecordSet(m_marmot, result.read());
			m_marmot.createDataSet(Globals.SHIP_TRACKS_REFINED, geomCol, srid, output, true);
		}
		catch ( Exception e ) {
			e.printStackTrace(System.err);
			return;
		}
		finally {
//			m_marmot.deleteDataSet(Globals.TEMP_SHIP_TRJ);
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
			new AssignGridCell(marmot).run();
			
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
				new AssignGridCell(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

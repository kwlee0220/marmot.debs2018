package debs2018;

import java.time.Duration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.MarmotServer;
import marmot.Plan;
import marmot.rset.RecordSets;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DrawSingleShipTravel implements Runnable {
	private final MarmotServer m_marmot;
	
	private DrawSingleShipTravel(MarmotServer marmot) {
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
			
			Plan plan = m_marmot.planBuilder("build_histogram")
								.load(Globals.SHIP_TRACKS)
								.filter("ship_id=='0xe472828f03b9e4cf516569db40725a615f8ac064'"
										+ "&& departure_port_name=='VALLETTA'")
								.expand("ts:long", initExpr, expr)
								.project(prjExpr)
								.build();
			DataSet result = m_marmot.createDataSet("tmp/single_ship_trip", "the_geom",
													"EPSG:4326", plan, true);
			
			RecordSets.observe(result.read())
				.buffer(2,1)
				.filter(l -> l.size() >= 2)
				.map(l -> Duration.ofMillis(l.get(1).getLong("ts", -1) - l.get(0).getLong("ts", -1)))
				.map(d -> d.toHours())
				.filter(h -> h > 2)
				.subscribe(System.out::println);
			
			// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
			DebsUtils.printPrefix(result, 5);
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
			new DrawSingleShipTravel(marmot).run();
			
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
				new DrawSingleShipTravel(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

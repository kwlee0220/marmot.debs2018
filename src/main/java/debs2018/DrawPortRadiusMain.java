package debs2018;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.MarmotServer;
import marmot.Plan;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DrawPortRadiusMain implements Runnable {
	private static final String RESULT = "debs/ports_radius";
	
	private final MarmotServer m_marmot;
	
	private DrawPortRadiusMain(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		try {
			Plan plan = m_marmot.planBuilder("draw_port_radius")
								.load("debs/ports")
								.transformCrs("the_geom", "EPSG:4326", "EPSG:3857", "the_geom")
								.expand("region:polygon", "region = ST_Buffer(the_geom, radius)")
								.project("region as the_geom, port_name, radius")
								.store(RESULT)
								.build();
			GeometryColumnInfo info = new GeometryColumnInfo("the_geom", "EPSG:3857");
			m_marmot.createDataSet(RESULT, info, plan, DataSetOption.FORCE);
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
			new DrawPortRadiusMain(marmot).run();
			
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
				new DrawPortRadiusMain(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

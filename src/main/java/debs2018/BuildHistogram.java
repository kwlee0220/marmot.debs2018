package debs2018;

import static marmot.optor.AggregateFunction.SUM;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.MarmotServer;
import marmot.Plan;
import utils.CommandLine;
import utils.CommandLineParser;
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
			Plan plan = m_marmot.planBuilder("build_histogram")
								.load(Globals.SHIP_TRACKS_REFINED)
								.expand("count:int", "count = 1")
								.groupBy("cell_id,depart_port,dest_port")
									.taggedKeyColumns("cell_pos")
									.workerCount(11)
									.aggregate(SUM("count").as("count"))
								.expand("x:int,y:int", "x = cell_pos.x; y=cell_pos.y;")
								.project("x,y,depart_port,dest_port,count")
								.store(Globals.SHIP_GRID_CELLS)
								.build();
			DataSet result = m_marmot.createDataSet(Globals.SHIP_GRID_CELLS, plan, true);
			
			// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
			SampleUtils.printPrefix(result, 5);
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

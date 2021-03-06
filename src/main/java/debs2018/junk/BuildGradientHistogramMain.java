package debs2018.junk;

import static marmot.optor.AggregateFunction.COUNT;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import debs2018.Globals;
import marmot.MarmotServer;
import marmot.MarmotServerBuilder;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SquareGrid;
import marmot.plan.Group;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildGradientHistogramMain implements Runnable {
	private final MarmotServer m_marmot;
	
	private BuildGradientHistogramMain(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		try {
			String prjExpr = "the_geom,ship_id,departure_port,timestamp,gradient";
			Size2d cellSize = GeoClientUtils.divide(Globals.BOUNDS, Globals.RESOLUTION);
			
			Plan plan = m_marmot.planBuilder("tag_gradient")
								.load(Globals.SHIP_TRACKS_LABELED)
								.assignGridCell("the_geom", new SquareGrid(Globals.BOUNDS, cellSize), false)
								.aggregateByGroup(Group.ofKeys("cell_id,gradient,arrival_port_calc")
														.withTags("cell_pos"),
													COUNT().as("count"))
								.expand("x:int,y:int", "x = cell_pos.x; y=cell_pos.y;")
								.project("x,y,gradient,arrival_port_calc,count")
								.store("tmp/result")
								.build();
			m_marmot.createDataSet("tmp/result", plan, StoreDataSetOptions.FORCE);
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

			MarmotServer marmot = new MarmotServerBuilder().forLocalMR().build();
			new BuildGradientHistogramMain(marmot).run();
			
			System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
		}
	}
	
	public static class Driver extends Configured implements Tool {
		@Override
		public int run(String[] args) throws Exception {
			try {
				MarmotServer marmot = MarmotServer.initialize(getConf());

				StopWatch watch = StopWatch.start();
				new BuildGradientHistogramMain(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

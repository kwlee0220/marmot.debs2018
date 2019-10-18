package debs2018;

import static marmot.optor.AggregateFunction.COUNT;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

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
public class BuildHistogram implements Runnable {
	private final MarmotServer m_marmot;
	
	private BuildHistogram(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		try {
			Size2d cellSize = GeoClientUtils.divide(Globals.BOUNDS, Globals.RESOLUTION);
			
			Plan plan = m_marmot.planBuilder("build_ship_trajectory")
								.load(Globals.SHIP_TRACKS_LABELED)
								.filter("arrival_port_calc != null && arrival_port_calc.length() > 0 ")
								.assignGridCell("the_geom", new SquareGrid(Globals.BOUNDS, cellSize), false)
								.takeByGroup(Group.ofKeys("traj_id,cell_id")
												.tags("cell_pos,departure_port,arrival_port_calc,ship_type"), 1)
								.aggregateByGroup(Group.ofKeys("cell_id,departure_port,arrival_port_calc,ship_type")
														.withTags("cell_pos"),
													COUNT().as("count"))
								.expand("x:int,y:int", "x = cell_pos.x; y=cell_pos.y;")
								.project("x,y,departure_port,arrival_port_calc,ship_type,count")
								.store(Globals.SHIP_GRID_CELLS)
								.build();
			m_marmot.createDataSet(Globals.SHIP_GRID_CELLS, plan, StoreDataSetOptions.FORCE);
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
			new BuildHistogram(marmot).run();
			
			System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
		}
	}
	
	public static class Driver extends Configured implements Tool {
		@Override
		public int run(String[] args) throws Exception {
			try {
				MarmotServer marmot = MarmotServer.initialize(getConf());

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

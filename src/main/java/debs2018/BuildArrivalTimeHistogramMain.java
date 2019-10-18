package debs2018;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import marmot.MarmotServer;
import marmot.MarmotServerBuilder;
import marmot.Plan;
import marmot.RecordScript;
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
public class BuildArrivalTimeHistogramMain implements Runnable {
	private final MarmotServer m_marmot;
	
	private BuildArrivalTimeHistogramMain(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		try {
			String initExpr = "$pat1 = ST_DTPattern('dd-MM-yy H:mm');"
							+ "$pat2 = ST_DTPattern('dd-MM-yy H:mm:ss');";
			String expr = "if ( timestamp.split(':').length == 2 ) {"
						+ "   ts = ST_DTToMillis(ST_DTParseLE(timestamp, $pat1)); }"
						+ "else { ts = ST_DTToMillis(ST_DTParseLE(timestamp, $pat2)); }";
			String expr2 = "if ( arrival_calc.split(':').length == 2 ) {"
						+ "   arrival_calc = ST_DTToMillis(ST_DTParseLE(arrival_calc, $pat1)); }"
						+ "else { arrival_calc = ST_DTToMillis(ST_DTParseLE(arrival_calc, $pat2)); }";
			String getRemainingTime = "remains_millis = Math.max(arrival_calc-ts, 0);";
			
			Size2d cellSize = GeoClientUtils.divide(Globals.BOUNDS, Globals.RESOLUTION);
			
			Plan plan;
			plan = m_marmot.planBuilder("build_eta_statistics")
							.load(Globals.SHIP_TRACKS_TIME)
							.filter("arrival_port_calc != null && arrival_port_calc.length() > 0 ")
							.expand("ts:long", RecordScript.of(initExpr, expr))
							.expand("arrival_calc:long", RecordScript.of(initExpr, expr2))
							.assignGridCell("the_geom", new SquareGrid(Globals.BOUNDS, cellSize), false)
							.expand("remains_millis:long", getRemainingTime)
							.expand("speed:int", "speed = Math.round(speed)")
							.aggregateByGroup(Group.ofKeys("cell_id,arrival_port_calc,speed")
													.withTags("cell_pos"),
												AVG("remains_millis"), COUNT())
							.expand("x:int,y:int", "x = cell_pos.x; y=cell_pos.y;")
							.expand("remains_millis:long", "remains_millis = Math.round(avg)")
							.project("x,y,arrival_port_calc,speed,remains_millis,count")
							.store(Globals.SHIP_GRID_CELLS_TIME)
							.build();
			m_marmot.createDataSet(Globals.SHIP_GRID_CELLS_TIME, plan, FORCE);
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
			new BuildArrivalTimeHistogramMain(marmot).run();
			
			System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
		}
	}
	
	public static class Driver extends Configured implements Tool {
		@Override
		public int run(String[] args) throws Exception {
			try {
				MarmotServer marmot = MarmotServer.initialize(getConf());

				StopWatch watch = StopWatch.start();
				new BuildArrivalTimeHistogramMain(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

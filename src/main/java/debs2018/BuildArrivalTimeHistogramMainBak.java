package debs2018;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import marmot.DataSetOption;
import marmot.MarmotServer;
import marmot.Plan;
import marmot.RecordScript;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SquareGrid;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildArrivalTimeHistogramMainBak implements Runnable {
	private final MarmotServer m_marmot;
	
	private BuildArrivalTimeHistogramMainBak(MarmotServer marmot) {
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
			String getRemainingTime = "remains_millis = Math.max(arrival_calc-ts, 0);";
			
			Size2d cellSize = GeoClientUtils.divide(Globals.BOUNDS, Globals.RESOLUTION);
			
			Plan plan;
			plan = m_marmot.planBuilder("build_eta_statistics")
							.load(Globals.SHIP_TRACKS_TIME_ADJUST)
							.filter("arrival_port_calc != null && arrival_port_calc.length() > 0 ")
							.expand("ts:long", RecordScript.of(initExpr, expr))
							.expand("arrival_calc:long", RecordScript.of(initExpr, expr2))
							.assignSquareGridCell("the_geom", new SquareGrid(Globals.BOUNDS, cellSize))
							.expand("remains_millis:long", getRemainingTime)
							.groupBy("cell_id,arrival_port_calc,ship_type")
								.tagWith("cell_pos")
								.aggregate(AVG("remains_millis"),COUNT())
							.expand("x:int,y:int", "x = cell_pos.x; y=cell_pos.y;")
							.expand("remains_millis:long", "remains_millis = Math.round(avg)")
							.project("x,y,arrival_port_calc,ship_type,remains_millis,count")
							.store(Globals.SHIP_GRID_CELLS_TIME)
							.build();
			m_marmot.createDataSet(Globals.SHIP_GRID_CELLS_TIME, plan, DataSetOption.FORCE);
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

			MarmotServer marmot = MarmotServer.initializeForLocalMR();
			new BuildArrivalTimeHistogramMainBak(marmot).run();
			
			System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
		}
	}
	
	public static class Driver extends Configured implements Tool {
		@Override
		public int run(String[] args) throws Exception {
			try {
				MarmotServer marmot = MarmotServer.initialize(getConf());

				StopWatch watch = StopWatch.start();
				new BuildArrivalTimeHistogramMainBak(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

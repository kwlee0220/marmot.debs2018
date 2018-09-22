package debs2018.junk;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import debs2018.Globals;
import marmot.DataSet;
import marmot.MarmotServer;
import marmot.Plan;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.SquareGrid;
import marmot.plan.GeomOpOption;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildCellLikelyhood implements Runnable {
	private final MarmotServer m_marmot;
	
	private BuildCellLikelyhood(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		try {
			CoordinateTransform ctrans = CoordinateTransform.get("EPSG:4326", "EPSG:3857");
			Envelope bounds = ctrans.transform(Globals.BOUNDS);
			Size2d cellSize = GeoClientUtils.divide(bounds, Globals.RESOLUTION);
			
			Plan plan = m_marmot.planBuilder("build_ship_trajectory")
								.loadSquareGridFile(new SquareGrid(bounds, cellSize), -1)
								.centroid("the_geom")
								.buffer("the_geom", Globals.RADIUS, GeomOpOption.OUTPUT("circle"))
								.spatialJoin("circle", Globals.SHIP_TRACKS_LABELED,
											"*-{circle},param.{the_geom as the_geom2,departure_port,arrival_port_calc}")
								.filter("arrival_port_calc != null && arrival_port_calc.length() > 0 ")
								.expand("mass:double", "mass = 1 / ST_Distance(the_geom,the_geom2)")
								.groupBy("cell_id,departure_port,arrival_port_calc")
									.tagWith("cell_pos")
									.aggregate(AggregateFunction.SUM("mass").as("mass"))
								.expand("x:int,y:int", "x = cell_pos.x; y=cell_pos.y;")
								.project("x,y,departure_port,arrival_port_calc,mass")
								.store(Globals.SHIP_GRID_CELLS)
								.build();
			DataSet ds = m_marmot.createDataSet(Globals.SHIP_GRID_CELLS, plan, true);
			
			DebsUtils.printPrefix(ds, 100);
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
			new BuildCellLikelyhood(marmot).run();
			
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
				new BuildCellLikelyhood(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

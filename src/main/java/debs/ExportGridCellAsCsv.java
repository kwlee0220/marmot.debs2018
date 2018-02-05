package debs;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.Column;
import marmot.DataSet;
import marmot.MarmotServer;
import marmot.RecordSet;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportGridCellAsCsv implements Runnable {
	private final MarmotServer m_marmot;
	
	private ExportGridCellAsCsv(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		try {
			DataSet tracks = m_marmot.getDataSet(Globals.SHIP_TRACKS_REFINED);
			try ( PrintWriter pw = new PrintWriter(new FileWriter("gridcell.info")) ) {
				Envelope bounds = tracks.getBounds();
				pw.printf("grid-bounds: %f %f %f %f%n", bounds.getMinX(), bounds.getMaxX(),
												bounds.getMinY(), bounds.getMaxY());
				pw.printf("grid-dimension: %s", Globals.RESOLUTION);
			}

			DataSet grid = m_marmot.getDataSet(Globals.SHIP_GRID_CELLS);
			try ( RecordSet rset = grid.read();
				PrintWriter pw = new PrintWriter(new FileWriter("gridcell.csv")) ) {
				String header = rset.getRecordSchema()
									.columnFStream()
									.map(Column::getName)
									.join(",", "#", "");
				pw.println(header);
				
				rset.stream()
					.map(rec -> Arrays.stream(rec.getAll())
										.map(Object::toString)
										.collect(Collectors.joining(",")))
					.forEach(pw::println);
			}
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
			new ExportGridCellAsCsv(marmot).run();
			
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
				new ExportGridCellAsCsv(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

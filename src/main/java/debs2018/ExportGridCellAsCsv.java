package debs2018;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
			try ( PrintWriter pw = new PrintWriter(new FileWriter("gridcell.info")) ) {
				Envelope bounds = Globals.BOUNDS;
				pw.printf("grid-bounds: %f %f %f %f%n", bounds.getMinX(), bounds.getMaxX(),
												bounds.getMinY(), bounds.getMaxY());
				pw.printf("grid-dimension: %s", Globals.RESOLUTION);
			}
			export(Globals.SHIP_GRID_CELLS, new File("gridcell.csv"));
			export(Globals.SHIP_GRID_CELLS_TIME, new File("gridcell_time.csv"));
		}
		catch ( Exception e ) {
			e.printStackTrace(System.err);
			return;
		}
	}
	
	private void export(String dsId, File csvFile) throws IOException {
		DataSet grid = m_marmot.getDataSet(dsId);
		try ( RecordSet rset = grid.read();
			PrintWriter pw = new PrintWriter(new FileWriter(csvFile)) ) {
			String header = rset.getRecordSchema()
								.columnFStream()
								.map(Column::name)
								.join(",", "#", "");
			pw.println(header);
			
			rset.stream()
				.map(rec -> Arrays.stream(rec.getAll())
									.map(Object::toString)
									.collect(Collectors.joining(",")))
				.forEach(pw::println);
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
			new ExportGridCellAsCsv(marmot).run();
			
			System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
		}
	}
	
	public static class Driver extends Configured implements Tool {
		@Override
		public int run(String[] args) throws Exception {
			try {
				MarmotServer marmot = MarmotServer.initialize(getConf());

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

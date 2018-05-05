package debs2018;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.vavr.Tuple;
import io.vavr.control.Try;
import marmot.DataSet;
import marmot.MarmotServer;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;
import utils.io.IOUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportTrajectoriesByPort implements Runnable {
	private final MarmotServer m_marmot;
	
	private ExportTrajectoriesByPort(MarmotServer marmot) {
		m_marmot = marmot;
	}

	@Override
	public void run() {
		Try.run(() -> Files.deleteIfExists(Paths.get("/home/kwlee/tmp/labels")));
		Try.run(() -> new File("/home/kwlee/tmp/labels").mkdir());
		
		Plan plan;
		plan = m_marmot.planBuilder("by_port")
						.load(Globals.SHIP_TRACKS_LABELED)
						.project("departure_port,arrival_port_calc")
						.distinct("departure_port,arrival_port_calc")
						.store("tmp/result")
						.build();
		DataSet output = m_marmot.createDataSet("tmp/result", plan, true);
		Map<String,List<String>> port2port;
		try ( RecordSet rset = output.read() ) {
			port2port = rset.fstream()
						.map(r -> Tuple.of(r.getString(0), r.getString(1)))
						.collectLeft(Maps.newHashMap(), (m,t) -> 
									m.computeIfAbsent(t._1, k-> Lists.newArrayList())
										.add(t._2));
		}
		
		for ( Map.Entry<String, List<String>> ent: port2port.entrySet() ) {
			String filter = String.format("departure_port == '%s'", ent.getKey());
			plan = m_marmot.planBuilder("by_port")
							.load(Globals.SHIP_TRACKS_LABELED)
							.filter(filter)
							.build();
			
			Map<String,List<Record>> fromPorts = Maps.newHashMap();
			try ( RecordSet rset = m_marmot.executeLocally(plan) ) {
				rset.forEachCopy(r -> {
					String arrivalPort = r.getString("arrival_port_calc");
					fromPorts.computeIfAbsent(arrivalPort, k -> Lists.newArrayList())
							.add(r);
				});
			}
			for ( Map.Entry<String, List<Record>> ent2: fromPorts.entrySet() ) {
				String file = String.format("/home/kwlee/tmp/labels/%s_%s.csv", ent.getKey(), ent2.getKey());
			
				try ( FileWriter writer = new FileWriter(new File(file));
						PrintWriter pw = new PrintWriter(writer) ) {
					
					ent2.getValue().stream().forEach(r -> {
						pw.println(FStream.of(r.getAll())
											.map(v -> "" + v)
											.join(","));
					});
				}
				catch ( IOException e ) {
					e.printStackTrace();
				}
			}
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
			new ExportTrajectoriesByPort(marmot).run();
			
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
				new ExportTrajectoriesByPort(marmot).run();
				System.out.printf("elapsed time=%s%n", watch.stopAndGetElpasedTimeString());
			}
			catch ( IllegalArgumentException e ) {
				System.out.println("" + e);
			}
			
			return 0;
		}
	}
}

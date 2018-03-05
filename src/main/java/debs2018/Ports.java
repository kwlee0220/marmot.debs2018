package debs2018;

import java.util.List;

import org.geotools.referencing.GeodeticCalculator;

import com.vividsolutions.jts.geom.Point;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.MarmotRuntime;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Ports {
	private final List<Port> m_ports;
	private final GeodeticCalculator m_gc = new GeodeticCalculator();
	
	private Ports(List<Port> ports) {
		m_ports = ports;
	}
	
	static Ports load(MarmotRuntime marmot) {
		List<Port> ports = marmot
				.getDataSet(Globals.PORTS)
				.read()
				.fstream()
				.map(Port::fromRecord)
				.toArrayList();
		
		return new Ports(ports);
	}
	
	Option<Port> findNearestValidPort(Point pt) {
		return FStream.of(m_ports)
					.map(port -> Tuple.of(port, calcDistance(pt, port.m_loc)))
					.filter(t -> Double.compare(t._2, t._1.m_radius) <= 0)
					.min(t -> t._2)
					.map(t -> t._1);
	}
	
	Tuple2<Port,Double> findNearestPort(Point pt) {
		return FStream.of(m_ports)
					.map(port -> Tuple.of(port, calcDistance(pt, port.m_loc)))
					.min(t -> t._2)
					.get();
	}
	
	private double calcDistance(Point pt1, Point pt2) {
		m_gc.setStartingGeographicPoint(pt1.getX(), pt1.getY());
		m_gc.setDestinationGeographicPoint(pt2.getX(), pt2.getY());
		return m_gc.getOrthodromicDistance();
	}
}

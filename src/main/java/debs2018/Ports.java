package debs2018;

import java.util.List;

import org.geotools.referencing.GeodeticCalculator;

import com.vividsolutions.jts.geom.Point;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.MarmotRuntime;
import marmot.Record;
import plaslab.debs2018.Port;
import plaslab.debs2018.gridcell.Point2f;
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
				.map(Ports::fromRecord)
				.toList();
		
		return new Ports(ports);
	}
	
	static Port fromRecord(Record record) {
		Point pt = (Point)record.getGeometry("the_geom");
		Point2f loc = new Point2f((float)pt.getX(), (float)pt.getY());
		float radius = (float)record.getDouble("radius", -1);
		
		return new Port(record.getString("port_name"), loc, radius);
	}
	
	Option<Port> findNearestValidPort(Point pt) {
		return findNearestValidPort(new Point2f((float)pt.getX(), (float)pt.getY()));
	}
	
	Option<Port> findNearestValidPort(Point2f loc) {
		return FStream.of(m_ports)
					.map(port -> Tuple.of(port, calcDistance(loc, port.location())))
					.filter(t -> Double.compare(t._2, t._1.radius()) <= 0)
					.min(t -> t._2)
					.map(t -> t._1);
	}
	
	Tuple2<Port,Double> findNearestPort(Point pt) {
		return findNearestPort(new Point2f((float)pt.getX(), (float)pt.getY()));
	}
	
	Tuple2<Port,Double> findNearestPort(Point2f loc) {
		return FStream.of(m_ports)
					.map(port -> Tuple.of(port, calcDistance(loc, port.location())))
					.min(t -> t._2)
					.get();
	}
	
	private double calcDistance(Point2f pt1, Point2f pt2) {
		m_gc.setStartingGeographicPoint(pt1.getX(), pt1.getY());
		m_gc.setDestinationGeographicPoint(pt2.getX(), pt2.getY());
		return m_gc.getOrthodromicDistance();
	}
}

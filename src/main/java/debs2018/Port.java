package debs2018;

import java.util.Objects;

import com.vividsolutions.jts.geom.Point;

import marmot.Record;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class Port {
	String m_name;
	Point m_loc;
	double m_radius;
	
	Port(String name, Point loc, double radius) {
		m_name = name;
		m_loc = loc;
		m_radius = radius;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		Port other = (Port)obj;
		return Objects.equals(m_name, other.m_name);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_name);
	}
	
	@Override
	public String toString() {
		return String.format("%s:%f:%f:%.1fkm)",
							m_name, m_loc.getX(), m_loc.getY(), m_radius/1000);
	}
	
	static Port fromRecord(Record record) {
		return new Port(record.getString("port_name"),
						(Point)record.getGeometry("the_geom"),
						record.getDouble("radius", -1));
	}
}
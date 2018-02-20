package debs2018;

import com.vividsolutions.jts.geom.Envelope;

import utils.Size2i;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Globals {
	public static final String SHIP_TRACKS = "debs/ship_tracks";
	public static final String PORTS = "debs/ports";
	public static final String SHIP_TRACKS_REFINED = "debs/ship_tracks_refined";
	public static final String TEMP_SHIP_TRJ = "tmp/debs/ship_traj";
	public static final String SHIP_GRID_CELLS = "debs/gridcells";
	public static final int WORKER_COUNT = 17;
	
	public static final Size2i RESOLUTION = new Size2i(700, 300);
	public static final Envelope BOUNDS = new Envelope(-6d,37d, 32d,45d);
}

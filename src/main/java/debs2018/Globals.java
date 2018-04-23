package debs2018;

import com.vividsolutions.jts.geom.Envelope;

import utils.Size2i;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Globals {
	public static final String SHIP_TRACKS = "debs/ship_tracks";
	public static final String SHIP_TRACKS_LABELED = "debs/ship_tracks_labeled";
	public static final String SHIP_GRID_CELLS = "debs/gridcells";
	public static final String SHIP_GRID_CELLS_TIME = "debs/gridcells_time";
	public static final int WORKER_COUNT = 17;
	
//	public static final Size2i RESOLUTION = new Size2i(700, 300);
//	public static final Size2i RESOLUTION = new Size2i(500, 200);
	public static final Size2i RESOLUTION = new Size2i(250, 100);
	public static final Envelope BOUNDS = new Envelope(-6d,37d, 31d,45d);
	public static final double RADIUS = 30 * 1000;	// 30km
}

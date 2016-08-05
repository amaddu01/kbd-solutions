package JoinProb;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

@SuppressWarnings("unchecked")
public class JoinWritable extends GenericWritable {

	private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
                Text.class
        };
    }
   
    public JoinWritable() {}
   
    public JoinWritable(Writable instance) {
        set(instance);
    }
    
	@Override
	protected Class<? extends Writable>[] getTypes() {
		// TODO Auto-generated method stub
		return CLASSES;
	}

}

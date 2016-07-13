package slipstream.extractor;

import java.lang.annotation.*;

/**
 * This class defines an annotation element that can be used to identify method
 * names and usage at runtime. The primary function of this annotation is for
 * use in annotating MBeans so that they can be self-documenting to the
 * management framework.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MethodDesc
{
    String description() default "";

    String usage() default "";
}

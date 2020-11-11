package software.amazon.aws.clients.swf.flux.guice;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.inject.BindingAnnotation;

/**
 * Should be attached to a Guice object containing a map of task list names to decider poller thread counts.
 * To be used with FluxModule.
 */
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@BindingAnnotation
public @interface TaskListDeciderPollerThreadCounts {
}

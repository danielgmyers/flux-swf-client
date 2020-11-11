package software.amazon.aws.clients.swf.flux.guice;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.inject.BindingAnnotation;

/**
 * Should be attached to a Guice object referencing a Double which should be used as the default base
 * in exponential backoff calculations (e.g. for calculating the retry time for a workflow step).
 *
 * Users are not required to provide this; if not specified, Flux's default base will be used.
 */
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@BindingAnnotation
public @interface ExponentialBackoffBase {
}

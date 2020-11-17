package software.amazon.aws.clients.swf.flux.guice;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.inject.BindingAnnotation;

/**
 * Should be attached to a Guice object identifying the SWF endpoint which should be used to contact SWF.
 * To be used with FluxModule.
 *
 * Users are not required to provide an endpoint; if an endpoint is not provided,
 * the SDK's default endpoint for the configured @SwfRegion will be used.
 */
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@BindingAnnotation
public @interface SwfEndpoint {
}

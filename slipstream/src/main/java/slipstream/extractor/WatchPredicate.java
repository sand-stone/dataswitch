package slipstream.extractor;

/**
 * Denotes a predicate that returns true if an input event matches the predicate
 * conditions.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public interface WatchPredicate<E>
{
    public boolean match(E event);
}

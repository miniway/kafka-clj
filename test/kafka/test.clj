; Copyright Julian Birch 
; http://www.colourcoding.net/blog/archive/2011/12/06/clojure-capturing-function-invocations.aspx

(ns kafka.test)


(defn ignore
  "Ignores the inputs and returns the outputs.  Useful as mock target."
  [& args] nil)

; Capture

(defn capture-invoke [{:keys [f captures returns returned]} args]
  (let [r (if (empty? @returns) (apply f args)
            (first @returns))]
    (swap! captures conj args)
    (swap! returns rest)
    (swap! returned conj r)
    r))

(defrecord capture-t [f captures returns returned]
  clojure.lang.IFn
  (invoke [this] (capture-invoke this []))
  (invoke [this a] (capture-invoke this [a]))
  (invoke [this a b] (capture-invoke this [a b]))
  (invoke [this a b c] (capture-invoke this [a b c]))
  (invoke [this a b c d] (capture-invoke this [a b c d]))
  (invoke [this a b c d e] (capture-invoke this [a b c d e]))
  (invoke [this a b c d e f] (capture-invoke this [a b c d e f]))
  (invoke [this a b c d e f g] (capture-invoke this [a b c d e f g]))
  (applyTo [this args]
    (clojure.lang.AFn/applyToHelper this args)))

(defn new-capture [f]
  (->capture-t f (atom []) (atom []) (atom [])))

(defn to-capture [[v f]]
  (new-capture (if (= f :v) (var-get v) f)))

(defn to-capture-map [h]
  (zipmap (keys h) (->> h (map to-capture))))

(defn captures [c]
  (-> c :captures deref))

(defn returned [c]
  (-> c :returned deref))

(defn times [c]
  (count (captures c)))

(defn returns [c r]
  (swap! (:returns c) conj r))


(defn with-captures-fn [bindings action]
  "Like with-redefs-fn, only you can call 'captures' on the redefined functions."
  (-> bindings
      to-capture-map
      (with-redefs-fn action)))

; Code ripped off from with-redefs
(defmacro with-captures
  "Like with-redefs, only you can call 'captures' on the redefined functions."
  [bindings & body]
  `(with-captures-fn ~(zipmap (map #(list `var %) (take-nth 2 bindings))
                              (take-nth 2 (next bindings)))
     (fn [] ~@body)))

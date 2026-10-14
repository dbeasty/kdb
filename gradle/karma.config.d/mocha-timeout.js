// Applied to every module's jsBrowserTest by the root build.gradle.kts. Keep in step with the
// node mocha timeout set there.
config.set({ client: { mocha: { timeout: 60000 } } });

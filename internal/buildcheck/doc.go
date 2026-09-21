// Package buildcheck holds repository-wide invariants that are cheaper to
// enforce as tests than as CI shell steps.
//
// There is no production code here. The tests walk the module from its root
// and assert things about the source itself, which means they run identically
// on a developer's machine and in CI, with no tool to install and no binary to
// execute beyond the test process.
package buildcheck

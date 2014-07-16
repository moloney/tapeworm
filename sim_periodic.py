'''Simulate the behavior of periodic backups'''
import os, sys, random
from datetime import datetime, timedelta
import numpy as np


def make_const_pdf(val=1.0):
    def const_pdf(x, limits):
        return val
    return const_pdf


def make_linear_pdf():
    def linear_pdf(x, limits):
        return ((x - limits[0]) / (limits[1] - limits[0]))
    return linear_pdf


def make_power_pdf(power=2):
    def power_pdf(x, limits):
        return (((x - limits[0]) ** power) / 
                ((limits[1] - limits[0]) ** power))
    return power_pdf

    
def sim(pdf, max_prob_scale, run_spacing, periodic_range, n_runs=None, n_files=None):
    '''Simulate the periodic backup process on a large chunk of files that 
    are created and then not modified (and thus only backed up by the 
    periodic backup process).
    
    Parameters
    ----------
    pdf : callable
        The probability density function gives a normalized probability for 
        a file being selected for backup for a given age (time since last 
        backup).
        
    max_prob_scale : float
        Scaling factor for the maximum probability. This is multiplied by 
        the fraction '(run_spacing / len(periodic_range))'.
        
    run_spacing : timedelta
        The time between each backup run
        
    periodic_range : tuple
        The minimum and maximum timedelta for the periodic range. The 
        probability of being selected is 0 below the min and 1 above the max.
        
    n_runs : int
        The number of runs to simulate. Defaults to just past one cycle of the
        periodic range.
        
    n_files : int
        The number of files to simulate. Defaults to 1000 times the number 
        of runs in one periodic cycle.
    '''
    # Convert timedeltas to floats
    run_spc_sec = run_spacing.total_seconds()
    periodic_rng_sec = (periodic_range[0].total_seconds(),
                        periodic_range[1].total_seconds())
    periodic_len_sec = periodic_rng_sec[1] - periodic_rng_sec[0]
    runs_per_cycle = run_spc_sec / periodic_len_sec
    
    # If the number of runs is not given, go to just past the maximum 
    # periodic range
    if n_runs is None:
        n_runs = int(periodic_rng_sec[1] / run_spc_sec) + 1
    print "Doing %d simulated backup runs" % n_runs
    
    # We need enough files to capture all of the effects at later points in 
    # the simulation
    if n_files is None:
        n_files = runs_per_cycle * 500
    print "Using a total of %d files" % n_files
    
    # Compute the maximum probability
    max_prob = (run_spc_sec / periodic_len_sec) * max_prob_scale
    print "Maximum probability of selecting a file is %f" % max_prob
    
    # Setup our storage arrays
    n_backups = np.zeros(n_runs)
    file_ages = np.zeros(n_files)
    
    # Do the simulation
    for run_idx in xrange(n_runs):
        file_ages += run_spc_sec
        for file_idx, file_age in enumerate(file_ages):
            if file_age < periodic_rng_sec[0]:
                continue
            elif file_age > periodic_rng_sec[1]:
                n_backups[run_idx] += 1
                file_ages[file_idx] = 0
            else:
                select_prob = pdf(file_age, periodic_rng_sec) * max_prob
                if random.random() < select_prob:
                    n_backups[run_idx] += 1
                    file_ages[file_idx] = 0
    
    print "Ratio of backups to files: %f" % (np.sum(n_backups) / n_files)
    return n_backups


if __name__ == '__main__':
    from pylab import plot, show
    # Found this works well for below settings. We want to balance out the 
    # two peaks that occur in the first cycle through the periodic range. 
    # The first peak is where the product of the PDF and the available 
    # population is at its maximum, the second is from files that have 
    # never been selected hitting the maximum age.
    pdf = make_power_pdf()
    pdf_scale = 11
    res = sim(pdf, 
              pdf_scale,
              timedelta(weeks=1), # Run once a week
              (timedelta(weeks=26), timedelta(weeks=104)), # Periodic range is 6-24 months
              n_runs=105, 
              n_files=50000)
    plot(res)
    show()
